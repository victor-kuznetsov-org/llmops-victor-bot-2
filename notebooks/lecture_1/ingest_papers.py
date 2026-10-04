import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml
from databricks.connect import DatabricksSession

NS = {"a": "http://www.w3.org/2005/Atom"}
URL = "https://export.arxiv.org/api/query?search_query=cat:cs.AI&sortBy=submittedDate&sortOrder=descending&start=0&max_results=20"


def fetch():
    with urllib.request.urlopen(URL, timeout=60) as r:
        root = ET.fromstring(r.read())
    rows = []
    for e in root.findall("a:entry", NS):
        rows.append(
            {
                "arxiv_id": e.find("a:id", NS).text.split("/")[-1],
                "title": " ".join(e.find("a:title", NS).text.split()),
                "summary": " ".join(e.find("a:summary", NS).text.split()),
                "authors": [x.find("a:name", NS).text for x in e.findall("a:author", NS)],
                "published": e.find("a:published", NS).text,
            }
        )
    return rows


if __name__ == "__main__":
    cfg = yaml.safe_load(Path("project_config.yml").read_text())["dev"]
    spark = DatabricksSession.builder.profile("student-bot-2").serverless(True).getOrCreate()
    rows = fetch()
    print(len(rows), "papers")
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").saveAsTable(f"{cfg['catalog']}.{cfg['schema']}.papers")
