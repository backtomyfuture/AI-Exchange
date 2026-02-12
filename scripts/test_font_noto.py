import sys
import logging
import os

logging.basicConfig(level=logging.WARNING)

def test_pdf_generation():
    print("Testing PDF generation with Noto Sans CJK SC...")
    
    try:
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
    except ImportError:
        print("WeasyPrint error.")
        return

    html_content = """
    <html>
        <head>
            <style>
                body {
                    font-family: 'Noto Sans CJK SC', 'Arial Unicode MS', sans-serif;
                    font-size: 24px;
                }
            </style>
        </head>
        <body>
            <h1>Font Test (Noto First)</h1>
            <p>This is a test. 这是一个测试。</p>
            <p>Test chars: 曹宇鹏黄慧扬</p>
        </body>
    </html>
    """
    
    output_path = '/app/test_noto.pdf'
    
    try:
        font_config = FontConfiguration()
        HTML(string=html_content).write_pdf(
            output_path,
            font_config=font_config
        )
        print(f"PDF generated successfully at {output_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_pdf_generation()
