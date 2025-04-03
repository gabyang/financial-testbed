Optimisation Ideas
1) Vary the embedding dimensions
2) Vary the sentence transformers used from MTEB
3) Vary the type of meta data stored together with the vector embedding
- Consider only essential metadata
- Consider also a field with a summarised version of the aritcle

4) Varying the types of chunking methodologies
- take note that chunking size is interrelated with embedding dimensions so pay close attention
For example, a study found recursive chunking with 100-token chunks and 15-token overlap achieved 98% precision/recall in a 768D embedding setup
- we will likely need to double check the precision/recall

The "Linq-AI-Research/Linq-Embed-Mistral" embedding model embeds into 4096 dimensions and has 7B parameters, requiring a minimum of 16GB of recommended RAM

Consider how to store the articles based on symbol. Everything needs to be paritioned/ordered by Symbol
