import unittest
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils import lark_app

class TestComplexLarkRendering(unittest.TestCase):

    def setUp(self):
        # HTML with complex elements: Tables, Colors, Lists, Links, Images
        self.complex_html = """
        <html>
        <body>
            <h1 style="color: #2E86C1;">Monthly Report</h1>
            <p>Here is the <strong>summary</strong> of our progress:</p>
            <ul>
                <li>Item 1: <em>Completed</em></li>
                <li>Item 2: <span style="text-decoration: line-through;">Cancelled</span></li>
            </ul>
            
            <h3>Data Table</h3>
            <table border="1">
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td>Revenue</td>
                    <td>$10,000</td>
                </tr>
                <tr>
                    <td>Growth</td>
                    <td>15%</td>
                </tr>
            </table>

            <p>Please refer to the chart below:</p>
            <img src="cid:chart_image_001" alt="Sales Chart" />
            
            <p>Visit our <a href="https://example.com">Dashboard</a> for more info.</p>
        </body>
        </html>
        """
        
        self.email_data = {
            "id": "mock_email_123",
            "subject": "FYI: Monthly Performance Report",
            "sender": "name='John Doe', email_address='john@example.com'",
            "to": ["name='Jane', email_address='jane@example.com'"],
            "cc": [],
            "received_at": "2023-10-27 10:00:00",
            "body": self.complex_html,
            "attachments": [{"name": "report.pdf"}, {"name": "data.xlsx"}]
        }
        
        self.classification = {
            "reasoning": "This is a regular business report."
        }
        
        self.draft = "Thank you for the report. Looks good."

    def test_html_to_lark_md_conversion(self):
        print("\n--- Testing HTML to Lark MD Conversion ---")
        md_output = lark_app.html_to_lark_md(self.complex_html)
        print(f"Generated Markdown:\n{md_output}")
        
        # Basic assertions
        self.assertIn("**Monthly Report**", md_output, "H1 should be bold") # Expected behavior after fix
        self.assertIn("[Dashboard](https://example.com)", md_output, "Links should be formatted")
        
    def test_card_structure(self):
        print("\n--- Testing Card Structure ---")
        card = lark_app.build_approval_card(
            "mock_email_123", 
            self.draft, 
            [{"chunk_text": "Preview text..."}], 
            self.email_data, 
            self.classification
        )
        print(json.dumps(card, indent=2, ensure_ascii=False))
        
        # Check if attachments are listed
        elements_str = json.dumps(card.get('elements', []), ensure_ascii=False)
        self.assertIn("report.pdf", elements_str)
        self.assertIn("data.xlsx", elements_str)

if __name__ == "__main__":
    unittest.main()
