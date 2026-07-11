import asyncio
import logging
from typing import List, Dict, Any
from src.domain.email_state import ProcessingOutcome
from src.safety.input_limits import InputLimitExceeded
from src.utils.circuit_breaker import circuit_breaker
from src.exchange_service import process_and_archive_email

logger = logging.getLogger("SelfHealing")

IMMEDIATE_STUCK_STATUSES = frozenset({"error", "delivery_failed"})
STALE_STUCK_STATUSES = frozenset({"ingested", "analyzed", "pending"})
STUCK_STATUSES = tuple(sorted(IMMEDIATE_STUCK_STATUSES | STALE_STUCK_STATUSES))
STALE_THRESHOLD_SECONDS = 1800  # 30 minutes


class SelfHealer:
    def __init__(self, ctx, interval_seconds: int = 900):
        self.ctx = ctx
        self.interval = interval_seconds
        self.is_running = False

    async def get_stuck_emails(self) -> List[Dict[str, Any]]:
        """
        Find emails that are either in 'error' state or stuck in intermediate states for too long.
        """
        try:
            async with self.ctx.db_manager.get_connection() as conn:
                async with conn.cursor() as cur:
                    query = """
                        SELECT id, status, subject, updated_at
                        FROM emails_log
                        WHERE status IN ('error', 'delivery_failed')
                           OR (status IN ('ingested', 'analyzed', 'pending')
                               AND updated_at < CURRENT_TIMESTAMP - INTERVAL '30 minutes')
                        ORDER BY updated_at ASC
                        LIMIT 20
                    """
                    await cur.execute(query)
                    return await cur.fetchall()
        except Exception as exc:
            logger.error(
                "Failed to query stuck emails for self-healing: error_type=%s",
                type(exc).__name__,
            )
            return []

    async def _release_claim(self, email_id: str) -> None:
        """Best-effort release that cannot overwrite a newer terminal state."""
        try:
            await self.ctx.db_manager.compare_and_set_status(
                email_id,
                expected=frozenset({"recovering"}),
                target="error",
            )
        except Exception as exc:
            logger.error(
                "Self-healing claim release failed: email_id=%s error_type=%s",
                email_id,
                type(exc).__name__,
            )

    async def reprocess_single(self, email_id: str) -> bool:
        """
        Reprocess a single email. Re-uses logic from recovery script but integrated.
        """
        logger.info("Self-healing: Attempting to reprocess email %s", email_id)

        claimed = False
        try:
            claimed = await self.ctx.db_manager.claim_self_healing(
                email_id,
                immediate=IMMEDIATE_STUCK_STATUSES,
                stale=STALE_STUCK_STATUSES,
                stale_after_seconds=STALE_THRESHOLD_SECONDS,
            )
            if not claimed:
                logger.info(
                    "Self-healing claim skipped: email_id=%s",
                    email_id,
                )
                return False

            # Step 1: Fetch email data from Exchange
            email_data = await self.ctx.exchange_client.get_email(email_id)
            if not email_data:
                logger.error(
                    "Self-healing could not fetch email: email_id=%s",
                    email_id,
                )
                await self._release_claim(email_id)
                return False
            
            email_data['id'] = email_id
            
            # Step 2: Run full processing pipeline without deleting durable state.
            # This uses the new fixed process_and_archive_email
            outcome = await process_and_archive_email(
                email_data,
                self.ctx,
                force_reprocess=True,
            )
            if outcome not in {
                ProcessingOutcome.PROCESSED,
                ProcessingOutcome.ARCHIVED,
            }:
                logger.warning(
                    "Self-healing did not recover email: email_id=%s outcome=%s",
                    email_id,
                    outcome.value if isinstance(outcome, ProcessingOutcome) else "invalid",
                )
                await self._release_claim(email_id)
                return False
            logger.info("Self-healing successfully recovered email %s", email_id)
            return True
        except InputLimitExceeded as exc:
            logger.error(
                "Self-healing input rejected: email_id=%s category=%s",
                email_id,
                exc.category,
            )
            if claimed:
                await self._release_claim(email_id)
            return False
        except Exception as exc:
            logger.error(
                "Self-healing recovery failed: email_id=%s error_type=%s",
                email_id,
                type(exc).__name__,
            )
            if claimed:
                await self._release_claim(email_id)
            return False

    async def run_healing_cycle(self):
        """
        Main healing logic loop.
        """
        # Check Circuit Breaker status
        if not circuit_breaker.can_proceed():
            # System is PAUSED. Check if we should attempt recovery probing.
            if circuit_breaker.should_attempt_recovery():
                logger.info("Self-healing: System is PAUSED but recovery timeout reached. Attempting PROBE.")
                stuck = await self.get_stuck_emails()
                if not stuck:
                    logger.info("Self-healing: No stuck emails to use as probe.")
                    return

                # Pick exactly ONE email as a probe
                probe_email = stuck[0]
                success = await self.reprocess_single(probe_email['id'])
                
                if success:
                    logger.info("Self-healing: 🌟 Probe SUCCESS! Reporting success to Circuit Breaker.")
                    if circuit_breaker.report_success():
                        # The CB itself might send a notification, or we can add one here.
                        from src.utils import lark_app
                        lark_app.send_system_notification(
                            title="✅ 系统服务已恢复 (System Recovered)",
                            content="自愈进程已通过探测邮件验证服务健康度。故障已排除，系统恢复自动处理新邮件。",
                            template="green"
                        )
                else:
                    logger.warning("Self-healing: Probe FAILED. System remains PAUSED.")
            else:
                logger.debug("Self-healing: System is PAUSED. Skipping cycle.")
            return

        # System is HEALTHY. Handle any individual errors.
        stuck = await self.get_stuck_emails()
        if not stuck:
            return

        logger.info(f"Self-healing: Found {len(stuck)} stuck emails. Processing batch of max 5.")
        
        # Process in a small batch to avoid hammering LLM if there's a backlog
        batch = stuck[:5]
        for email in batch:
            await self.reprocess_single(email['id'])
            # Small delay between recoveries
            await asyncio.sleep(2)

    async def start(self):
        """Start the background healing task."""
        if self.is_running:
            return
        
        self.is_running = True
        logger.info(f"Self-healing worker started (Interval: {self.interval}s)")
        
        while self.is_running:
            try:
                await self.run_healing_cycle()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "Unexpected error in self-healing cycle: error_type=%s",
                    type(exc).__name__,
                )
            
            await asyncio.sleep(self.interval)
        
        logger.info("Self-healing worker stopped.")

    def stop(self):
        self.is_running = False
