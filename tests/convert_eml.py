# Task: Check project progress since last week
# 1. Create task.md to track progress.

import sys
import os
import email
from email import policy
from email.parser import BytesParser

sys.path.append(os.getcwd())

from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf

def convert_eml(eml_path, output_path):
    print(f"Reading {eml_path}...")
    with open(eml_path, 'rb') as f:
        msg = BytesParser(policy=policy.default).parse(f)

    # Extract body
    body_html = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
            if ctype == 'text/html' and 'attachment' not in cdispo:
                body_html = part.get_content()
                break
    else:
        body_html = msg.get_content()

    # Extract headers
    sender = str(msg.get("From"))
    to = str(msg.get("To"))
    cc = str(msg.get("Cc"))
    subject = str(msg.get("Subject"))
    date = str(msg.get("Date"))

    email_data = {
        "subject": subject,
        "sender": sender,
        "to": to.split(", ") if to else [],
        "cc": cc.split(", ") if cc else [],
        "received_at": date,
        "body": body_html,
        "attachments": [] # Simplified for this demo
    }

    print("Rendering HTML...")
    html_content = render_email_html(email_data)
    
    print("Generating PDF with WeasyPrint...")
    pdf_bytes = convert_html_to_pdf(html_content)
    
    with open(output_path, "wb") as f:
        f.write(pdf_bytes)
        
    print(f"Saved PDF to {output_path}")

if __name__ == "__main__":
    convert_eml("tests/fixtures/nas.eml", "tests/fixtures/nas.pdf")
