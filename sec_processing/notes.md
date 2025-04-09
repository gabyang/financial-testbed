Immediately retrieve data from online instead of parsing the SEC file - fastest way to do it (process_sec_online.py)
For the sake of the project, we can possibly embeded the sec files in a graph/tree format and store it somewhere (process_sec_offline.py)

Update: It is proving extremely difficult to retrieve data from online easily
Why is it difficult? Many tools provides a way to retrieve filings and content in json format
However, the tough part is:
1) extraction of earnings per share 
- some companies like Visa have multiple stocks which makes it have multiple earnings per share
- minute but important differences in the way earnings per share is shown in the document itself between companies (non-standardised)
2) Extraction of other probably relevant details
- Revenue figures, net income values, forward-looking statement indicators, risk factor keywords
- Management discussion highlights, stock price reaction metrics

Failed attempts:
1) Manually retrieve 10Q data with requests library in python
2) Regex parsing
3) Standard python Edgar libraries that are available

Potential to work:
1) [Sec-Parser](https://sec-parser.readthedocs.io/en/latest/)
2) [Edgartools](https://github.com/dgunning/edgartools)

Naive Implementation using [Sec-Parser](https://sec-parser.readthedocs.io/en/latest/) as this is built for the purposes of ML, AI and LLM use.
There's no simple way to do this. Hence, we'll start with the naive [memwalker implementation](https://arxiv.org/abs/2310.05029) which is essentially creating summaries of chunks of text, then traversing down the tree of summaries to find the content.
Schema of the document_nodes
```sql
document_nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    parent_id VARCHAR(255)),
    section_type VARCHAR(50)), -- e.g., RISK_FACTORS, MANAGEMENT_DISCUSSION
    summary TEXT,
    raw_text TEXT,
    embedding vector(384)), -- For all-MiniLM-L6-v2 embeddings
    metadata JSONB -- Contains ticker, section depth, HTML tags
)
```

Limitation of this technique causes the nodes to lose context, fidelity, key facts and semantics so we need more than just summarization, consider the other core NLP techniques.
However, if this is not the bottleneck of the overall workloads, we will keep this implementation. The approach does not require the use of a vector store, so we'll store it in a jsonB field.
