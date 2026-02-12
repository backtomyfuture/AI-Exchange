import logging
import io
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Optional import for WeasyPrint (requires system libraries like pango)
try:
    from weasyprint import HTML, CSS
    from weasyprint.text.fonts import FontConfiguration
    WEASYPRINT_AVAILABLE = True
except (ImportError, OSError) as e:
    WEASYPRINT_AVAILABLE = False
    _WEASYPRINT_ERROR = str(e)
    # Define dummy classes/functions if WeasyPrint is not available to avoid NameError
    class HTML:
        def __init__(self, *args, **kwargs): pass
        def write_pdf(self, *args, **kwargs): raise RuntimeError("WeasyPrint not available")
    class CSS:
        def __init__(self, *args, **kwargs): pass
    class FontConfiguration:
        def __init__(self, *args, **kwargs): pass


# Font Path Configuration
# In Docker, we put fonts in /usr/share/fonts/truetype/custom
# WeasyPrint relies on FontConfig/Pango to find fonts.
# We don't need to manually load the file if the system finds it.
# We just need to make sure the CSS font-family matches the font name.
# "Arial Unicode MS" is usually the family name for "Arial Unicode.ttf".

def convert_html_to_pdf(html_content: str) -> bytes:
    """
    将 HTML 内容转换为 PDF 字节流
    
    Args:
        html_content: HTML 字符串
    
    Returns:
        PDF 文件的字节流 (bytes)
        
    Raises:
        RuntimeError: 如果WeasyPrint不可用
    """
    if not WEASYPRINT_AVAILABLE:
        raise RuntimeError(
            f"WeasyPrint is not available: {_WEASYPRINT_ERROR}\n"
            "Please install system dependencies: brew install pango cairo glib"
        )

    if not html_content:
        return b""

    try:
        # Define CSS to force usage of Chinese-capable font
        # Assuming the font file 'Arial Unicode.ttf' installs 'Arial Unicode MS'
        
        # Suppress "Ignored `border-*:solid windowtext 1.0pt` at ... invalid value" warnings
        # by replacing the MS Word specific 'windowtext' color with standard 'black'
        if html_content:
            html_content = html_content.replace('windowtext', 'black')

        font_config = FontConfiguration()
        
        css_string = """
            @font-face {
                font-family: 'Arial Unicode MS';
                src: local('Arial Unicode MS');
            }
            body { 
                font-family: 'Noto Sans CJK SC', 'Noto Sans SC', 'Arial Unicode MS', 'Microsoft YaHei', sans-serif; 
                font-size: 14px;
                line-height: 1.5;
                margin: 0;
            }
            @page {
                size: A4;
                margin: 1.5cm;
            }
            /* Structure */
            .header {
                margin-bottom: 20px;
                border-bottom: 2px solid #333;
                padding-bottom: 15px;
                /* Removed page-break-after: avoid; to prevent potential layout engine issues */
            }
            .subject {
                font-size: 18px;
                font-weight: bold;
                margin-top: 15px;
                display: block;
                color: #000;
            }
            .body {
                padding-top: 10px;
            }
            /* Content Handling */
            img {
                max-width: 100%;
                height: auto;
                page-break-inside: avoid;
            }
            table {
                width: 100% !important;
                table-layout: fixed; /* 强制表格宽度固定，防止溢出 */
                border-collapse: collapse;
                page-break-inside: auto;
                word-wrap: break-word;
            }
            tr {
                page-break-inside: avoid;
                page-break-after: auto;
            }
            td, th {
                word-break: break-all; /* 允许在单词内换行，防止长字符串撑破表格 */
                overflow-wrap: break-word;
                border: 1px solid #ddd;
                padding: 4px;
            }
            /* Preformatted text handling */
            pre {
                white-space: pre-wrap;
                word-wrap: break-word;
            }
        """
        
        # 转换 HTML 到 PDF 字节流
        pdf_bytes = HTML(string=html_content).write_pdf(
            stylesheets=[CSS(string=css_string, font_config=font_config)],
            font_config=font_config
        )
        
        return pdf_bytes
        
    except Exception as e:
        logger.error(f"Error generating PDF (WeasyPrint): {e}", exc_info=True)
        # Re-raise the exception instead of returning empty bytes
        raise

