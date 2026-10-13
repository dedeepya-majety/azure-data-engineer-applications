import asyncio
import re
import shutil
import markdown
from playwright.async_api import async_playwright

md_path = '/Users/sailikhithkanuparthi/Downloads/career/job-search/about_dedeepya/DEDEEPYA_LAKEHOUSE_AND_DATA_ENGINEERING_MEGA_GUIDE.md'
pdf_path = '/Users/sailikhithkanuparthi/Downloads/career/job-search/about_dedeepya/DEDEEPYA_LAKEHOUSE_AND_DATA_ENGINEERING_MEGA_GUIDE.pdf'
artifact_path = '/Users/sailikhithkanuparthi/.gemini/antigravity-ide/brain/024a3da3-85d4-47aa-bd77-2e01c43e41cc/DEDEEPYA_LAKEHOUSE_AND_DATA_ENGINEERING_MEGA_GUIDE.pdf'

with open(md_path, 'r', encoding='utf-8') as f:
    text = f.read()

def repl_mermaid(m):
    return f"<div class='mermaid'>\n{m.group(1)}\n</div>"

processed_text = re.sub(r'```mermaid\n(.*?)```', repl_mermaid, text, flags=re.DOTALL)

html_content = markdown.markdown(processed_text, extensions=['tables', 'fenced_code', 'toc'])

full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset='utf-8'>
<title>Databricks Lakehouse & Azure Data Engineering Master Preparation Guide</title>
<script src='https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js'></script>
<script>
  mermaid.initialize({{ startOnLoad: true, theme: 'neutral' }});
</script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
  
  @page {{
    size: A4;
    margin: 18mm 15mm 18mm 15mm;
  }}

  body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    color: #1e293b;
    line-height: 1.55;
    font-size: 10pt;
    padding: 0;
    margin: 0;
  }}

  h1 {{
    font-size: 18pt;
    font-weight: 700;
    color: #0f172a;
    border-bottom: 2px solid #0284c7;
    padding-bottom: 6px;
    margin-top: 0;
    margin-bottom: 12px;
    page-break-after: avoid;
  }}

  h2 {{
    font-size: 13pt;
    font-weight: 700;
    color: #0369a1;
    border-bottom: 1px solid #e2e8f0;
    padding-bottom: 4px;
    margin-top: 22px;
    margin-bottom: 10px;
    page-break-after: avoid;
  }}

  h3 {{
    font-size: 11.5pt;
    font-weight: 600;
    color: #0f172a;
    margin-top: 16px;
    margin-bottom: 6px;
    page-break-after: avoid;
  }}

  h4 {{
    font-size: 10.5pt;
    font-weight: 600;
    color: #334155;
    margin-top: 12px;
    margin-bottom: 4px;
    page-break-after: avoid;
  }}

  p, li {{
    margin-top: 0;
    margin-bottom: 7px;
    color: #334155;
  }}

  blockquote {{
    margin: 12px 0;
    padding: 8px 16px;
    background: #f0f9ff;
    border-left: 4px solid #0284c7;
    border-radius: 0 6px 6px 0;
    color: #0369a1;
    font-size: 9.5pt;
  }}

  blockquote p {{
    margin: 0;
    color: #0369a1;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 14px 0;
    font-size: 8.5pt;
    page-break-inside: avoid;
  }}

  th {{
    background: #0f172a;
    color: #ffffff;
    font-weight: 600;
    text-align: left;
    padding: 7px 9px;
    border: 1px solid #0f172a;
  }}

  td {{
    padding: 6px 9px;
    border: 1px solid #cbd5e1;
    vertical-align: top;
  }}

  tr:nth-child(even) td {{
    background: #f8fafc;
  }}

  pre {{
    background: #0f172a;
    color: #f8fafc;
    padding: 10px 12px;
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 8pt;
    line-height: 1.4;
    overflow-x: auto;
    margin: 10px 0;
    page-break-inside: avoid;
  }}

  code {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 8pt;
    background: #f1f5f9;
    color: #0284c7;
    padding: 2px 4px;
    border-radius: 4px;
  }}

  pre code {{
    background: transparent;
    color: inherit;
    padding: 0;
  }}

  .mermaid {{
    margin: 16px 0;
    text-align: center;
    page-break-inside: avoid;
  }}

  hr {{
    border: none;
    border-top: 1px solid #e2e8f0;
    margin: 18px 0;
  }}
</style>
</head>
<body>
{html_content}
</body>
</html>"""

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.set_content(full_html, wait_until='networkidle')
        await asyncio.sleep(2.5)  # Allow Mermaid SVGs to render
        await page.pdf(
            path=pdf_path,
            format='A4',
            margin={'top': '18mm', 'bottom': '18mm', 'left': '15mm', 'right': '15mm'},
            print_background=True,
            display_header_footer=True,
            header_template="<div style='font-size: 8pt; color: #94a3b8; width: 100%; text-align: right; padding-right: 15mm; font-family: Inter, sans-serif;'>Dedeepya Majety &bull; Databricks Lakehouse & Azure Data Engineering Mega Guide</div>",
            footer_template="<div style='font-size: 8pt; color: #94a3b8; width: 100%; text-align: center; font-family: Inter, sans-serif;'><span class='pageNumber'></span> of <span class='totalPages'></span></div>"
        )
        await browser.close()
        print('PDF generated successfully at:', pdf_path)
        shutil.copyfile(pdf_path, artifact_path)
        print('Copied to artifact path:', artifact_path)

asyncio.run(main())
