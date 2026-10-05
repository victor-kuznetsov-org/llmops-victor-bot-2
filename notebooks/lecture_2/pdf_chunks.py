import os
import urllib.request
from pathlib import Path

import pymupdf
import yaml
from databricks.connect import DatabricksSession

cfg = yaml.safe_load(Path("project_config.yml").read_text())["dev"]
spark = DatabricksSession.builder.profile("student-bot-2").serverless(True).getOrCreate()
table = f"{cfg['catalog']}.{cfg['schema']}"
papers = spark.table(f"{table}.papers").select("arxiv_id", "title").limit(8).collect()

tmp = Path(os.environ.get("TMPDIR", "/tmp"))
rows = []
for p in papers:
    pdf = tmp / f"{p.arxiv_id}.pdf"
    if len({r[0] for r in rows}) == 2:
        break
    try:
        with urllib.request.urlopen(f"https://arxiv.org/pdf/{p.arxiv_id}", timeout=120) as r:
            data = r.read()
        pdf.write_bytes(data)
    except Exception as e:
        print("skip", p.arxiv_id, e)
        continue
    text = "\n".join(page.get_text() for page in pymupdf.open(pdf))
    paras = [" ".join(x.split()) for x in text.split("\n\n")]
    paras = [x for x in paras if len(x) > 50]
    for i, para in enumerate(paras):
        rows.append((p.arxiv_id, p.title, i, para))
    print(p.arxiv_id, len(paras), "paragraphs")

df = spark.createDataFrame(rows, "arxiv_id string, title string, chunk_id int, text string")
name = f"{table}.pdf_chunks"
df.write.mode("overwrite").saveAsTable(name)
spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(spark.table(name).count(), "chunks")
