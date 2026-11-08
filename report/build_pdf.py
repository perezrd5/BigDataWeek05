from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
from pypdf import PdfReader
import pypdfium2 as pdfium
from report_content import PAGES, REFERENCES

root = Path(__file__).parent
body = ParagraphStyle('Body', fontName='Times-Roman', fontSize=12, leading=18, spaceAfter=10)
heading = ParagraphStyle('Heading', fontName='Times-Bold', fontSize=15, leading=19, spaceAfter=14)
title = ParagraphStyle('Title', fontName='Times-Bold', fontSize=22, leading=28, alignment=TA_CENTER, spaceAfter=24)
reference = ParagraphStyle('Reference', parent=body, fontSize=11, leading=15, spaceAfter=18)
story = [Spacer(1, 90), Paragraph('Scalable ETL and MLOps Pipeline', title), Paragraph('Python Data Processing and Model Deployment', ParagraphStyle('Subtitle', parent=body, alignment=TA_CENTER)), Spacer(1, 48)]
for line in [
    'Student name:  Doug Perez',
    'Course:  DDS8530:  Big Data Integration',
    'Instructor: Seyedmohsen Hosseini',
    'Submission date:  November 8, 2026',
]:
    story.append(Paragraph(line, body))
for name, paragraphs in PAGES:
    story.extend([PageBreak(), Paragraph(name, heading)])
    story.extend(Paragraph(escape(text), body) for text in paragraphs)
story.extend([PageBreak(), Paragraph('References', heading)])
for text, url in REFERENCES:
    story.append(Paragraph(escape(text) + '<br/><link href="' + url + '">' + url + '</link>', reference))

def footer(canvas, doc):
    canvas.setFont('Times-Roman', 10)
    canvas.drawRightString(540, 40, str(doc.page))

path = root/'DDS8530_Perez_Assignment05.pdf'
SimpleDocTemplate(str(path), pagesize=(612,792), rightMargin=72, leftMargin=72, topMargin=62, bottomMargin=62, title='Scalable ETL and MLOps Pipeline', author='').build(story, onFirstPage=footer, onLaterPages=footer)
reader = PdfReader(path)
assert len(reader.pages) == 7, f'Expected title + five body + references; got {len(reader.pages)}'
qa = root/'qa'
qa.mkdir(exist_ok=True)
pdf = pdfium.PdfDocument(str(path))
for index, page in enumerate(pdf):
    page.render(scale=1.5).to_pil().save(qa/f'page-{index+1}.png')
print('Verified 7 pages: title, 5 body pages, references')
