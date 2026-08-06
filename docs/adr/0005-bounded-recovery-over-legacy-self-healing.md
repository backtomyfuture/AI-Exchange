# Keep recovery bounded and external effects manual

The legacy `SelfHealer` periodic batch reprocessing loop will not be restored. The service retains durable retries and explicit operator requeue for known-safe work, while an outcome-unknown Feishu or Exchange external effect remains in manual review; blindly replaying it could duplicate a notification or email.
