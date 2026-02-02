import logging
import io
import os
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration

logger = logging.getLogger(__name__)

# Font Path Configuration
# In Docker, we put fonts in /usr/share/fonts/truetype/custom
# WeasyPrint relies on FontConfig/Pango to find fonts.
# We don't need to manually load the file if the system finds it.
# We just need to make sure the CSS font-family matches the font name.
# "Arial Unicode MS" is usually the family name for "Arial Unicode.ttf".

def convert_html_to_pdf(html_content: str) -> bytes:
    """
    Convert HTML string to PDF bytes using WeasyPrint.
    Uses 'Arial Unicode MS' or fallback for Chinese support.
    """
    if not html_content:
        return b""

    try:
        # Define CSS to force usage of Chinese-capable font
        # Assuming the font file 'Arial Unicode.ttf' installs 'Arial Unicode MS'
        font_config = FontConfiguration()
        
        css_string = """
            @font-face {
                font-family: 'Arial Unicode MS';
                src: local('Arial Unicode MS');
            }
            body { 
                font-family: 'Arial Unicode MS', 'Microsoft YaHei', sans-serif; 
                font-size: 14px;
            }
            @page {
                size: A4;
                margin: 2cm;
            }
            img {
                max-width: 100%;
                height: auto;
            }
        """
        
        pdf_bytes = HTML(string=html_content).write_pdf(
            stylesheets=[CSS(string=css_string, font_config=font_config)],
            font_config=font_config
        )
        
        return pdf_bytes
        
    except Exception as e:
        logger.error(f"Error generating PDF (WeasyPrint): {e}", exc_info=True)
        # Fallback simplistic error PDF text is overkill here, just return empty or re-raise
        return b""
