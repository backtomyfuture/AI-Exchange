import sys
import logging
import os

# Configure logging to capture WeasyPrint warnings
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('weasyprint')
logger.setLevel(logging.WARNING)

def test_pdf_generation():
    print("Testing PDF generation with Chinese characters...")
    
    try:
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
    except ImportError:
        print("WeasyPrint not installed. This script must be run inside the Docker container where dependencies are installed.")
        return

    html_content = """
    <html>
        <head>
            <style>
                @font-face {
                    font-family: 'Arial Unicode MS';
                    src: local('Arial Unicode MS');
                }
                body {
                    font-family: 'Noto Sans CJK SC', 'Noto Sans SC', 'Arial Unicode MS', 'Microsoft YaHei', sans-serif;
                    font-size: 24px;
                }
            </style>
        </head>
        <body>
            <h1>Font Test / 字体测试</h1>
            <p>This is a test. 这是一个测试。</p>
            <p>Test chars from logs: 曹宇鹏黄慧扬</p>
        </body>
    </html>
    """
    
    output_path = '/app/test_output.pdf' if os.path.exists('/app') else 'test_output.pdf'
    
    try:
        font_config = FontConfiguration()
        HTML(string=html_content).write_pdf(
            output_path,
            font_config=font_config
        )
        print(f"PDF generated successfully at {output_path}")
        print("Check logs above for any '.notdef glyph' warnings. If none, fonts are working.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_pdf_generation()
