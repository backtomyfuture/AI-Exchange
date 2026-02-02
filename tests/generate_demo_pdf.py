import sys
import os
sys.path.append(os.getcwd())

from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf

def generate_demo():
    # Mock a complex email
    mock_data = {
        "subject": "周报: 项目进度汇报 (Weekly Report)",
        "sender": "Jarod <jarod@example.com>",
        "to": ["Manager <manager@example.com>", "Team <team@example.com>"],
        "cc": ["Boss <boss@example.com>"],
        "received_at": "2026-01-31 10:00:00",
        "body": """
        <div style="font-family: Arial, sans-serif;">
            <h2 style="color: #2c3e50;">项目进度汇报</h2>
            <p>各位好，</p>
            <p>本周主要完成了以下工作：</p>
            <ul>
                <li><strong>后端开发</strong>: 完成了 PDF 生成功能的初步实现。</li>
                <li><strong>前端联调</strong>: 修复了 Lark 卡片显示异常的问题。</li>
            </ul>
            
            <h3 style="color: #2980b9;">详细数据</h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>任务模块</th>
                    <th>状态</th>
                    <th>进度</th>
                </tr>
                <tr>
                    <td>Docker环境</td>
                    <td><span style="color: green;">Done</span></td>
                    <td>100%</td>
                </tr>
                <tr>
                    <td>PDF引擎</td>
                    <td><span style="color: orange;">In Progress</span></td>
                    <td>80%</td>
                </tr>
            </table>
            
            <p style="margin-top: 20px;">
                <span style="color: red;">注意：</span> 下周我们将重点优化 PDF 的渲染效果。
            </p>
            
            <hr>
            <p style="font-size: 12px; color: #7f8c8d;">此邮件由 AI 助手自动生成。</p>
        </div>
        """
    }
    
    print("Rendering HTML...")
    html = render_email_html(mock_data)
    
    print("Converting to PDF...")
    pdf_bytes = convert_html_to_pdf(html)
    
    output_path = "demo_fpdf2_output.pdf"
    with open(output_path, "wb") as f:
        f.write(pdf_bytes)
    
    print(f"Generated demo PDF at: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    generate_demo()
