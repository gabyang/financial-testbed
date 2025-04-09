import openai
import json
import textwrap

class MemWalkerSystem:
    def __init__(self, api_key, summarization_model="gpt-3.5-turbo", reasoning_model="gpt-4"):
        self.api_key = api_key
        openai.api_key = api_key
        self.summarization_model = summarization_model
        self.reasoning_model = reasoning_model
        self.tree = None
        self.visited_nodes = []
        self.working_memory = ""
        
    def chunk_text(self, text, max_chunk_size=8000):
        """Split text into manageable chunks."""
        return textwrap.wrap(text, max_chunk_size, break_long_words=False, break_on_hyphens=False)
    
    def summarize_chunk(self, chunk):
        """Generate a summary for a text chunk using the summarization model."""
        prompt = f"Please provide a concise summary of the following text:\n\n{chunk}"
        response = openai.ChatCompletion.create(
            model=self.summarization_model,
            messages=[{"role": "system", "content": "You are a helpful assistant that summarizes text."},
                      {"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    
    def build_tree(self, text):
        """Build a hierarchical tree from the input text."""
        # First level: chunk the entire text
        chunks = self.chunk_text(text)
        
        # Create the root node
        tree = {
            "id": "root",
            "summary": "Root of the document tree",
            "children": []
        }
        
        # Process each chunk into a first-level node
        for i, chunk in enumerate(chunks):
            summary = self.summarize_chunk(chunk)
            node = {
                "id": f"chunk_{i}",
                "summary": summary,
                "content": chunk,
                "children": []
            }
            tree["children"].append(node)
        
        # If we have many chunks, create intermediate summary nodes
        if len(chunks) > 10:
            # Group first-level nodes and create second-level summary nodes
            grouped_nodes = [tree["children"][i:i+5] for i in range(0, len(tree["children"]), 5)]
            tree["children"] = []
            
            for i, group in enumerate(grouped_nodes):
                group_content = "\n\n".join([node["summary"] for node in group])
                group_summary = self.summarize_chunk(group_content)
                
                group_node = {
                    "id": f"group_{i}",
                    "summary": group_summary,
                    "children": group
                }
                tree["children"].append(group_node)
        
        self.tree = tree
        return tree
    
    def navigate_tree(self, query):
        """Navigate the tree to find relevant information for a query."""
        if not self.tree:
            return "Tree not built. Please build the tree first."
        
        # Start at the root
        current_node = self.tree
        self.visited_nodes = [current_node["id"]]
        self.working_memory = ""
        
        while True:
            # Determine which child node to visit next
            navigation_prompt = self._create_navigation_prompt(current_node, query)
            
            response = openai.ChatCompletion.create(
                model=self.reasoning_model,
                messages=[
                    {"role": "system", "content": "You are a navigation assistant helping find relevant information in a document."},
                    {"role": "user", "content": navigation_prompt}
                ]
            )
            
            decision = response.choices[0].message.content
            
            # Parse the decision
            if "ANSWER:" in decision:
                # Extract the final answer
                answer = decision.split("ANSWER:")[1].strip()
                return answer
            
            if "NAVIGATE:" in decision:
                # Extract the node to navigate to
                next_node_id = decision.split("NAVIGATE:")[1].strip()
                
                # Find the node
                next_node = self._find_node_by_id(current_node, next_node_id)
                if next_node:
                    current_node = next_node
                    self.visited_nodes.append(current_node["id"])
                    
                    # Update working memory
                    if "content" in current_node:
                        # If we've reached a leaf node with content
                        self.working_memory += f"\nRelevant content: {current_node['summary']}\n"
                else:
                    return f"Navigation error: Could not find node {next_node_id}"
            else:
                return "Navigation error: Could not parse the decision"
    
    def _create_navigation_prompt(self, node, query):
        """Create a prompt for the reasoning model to navigate the tree."""
        prompt = f"""
        QUERY: {query}
        
        CURRENT NODE: {node['id']} - {node['summary']}
        
        WORKING MEMORY (information gathered so far):
        {self.working_memory}
        
        VISITED NODES: {', '.join(self.visited_nodes)}
        
        AVAILABLE CHILD NODES:
        """
        
        if "children" in node and node["children"]:
            for i, child in enumerate(node["children"]):
                prompt += f"{i+1}. {child['id']} - {child['summary']}\n"
            
            prompt += """
            Based on the query and the information available, what would you like to do?
            
            1. NAVIGATE: [node_id] - Navigate to a specific child node
            2. ANSWER: [your answer] - Provide an answer based on the information gathered
            
            Please provide your decision.
            """
        else:
            # We've reached a leaf node
            if "content" in node:
                prompt += f"""
                This is a leaf node with the following content:
                
                {node['content']}
                
                Based on the query and all the information gathered, please provide an answer.
                Format: ANSWER: [your comprehensive answer]
                """
            else:
                prompt += """
                This node has no children and no content.
                Based on the information gathered so far, please provide an answer.
                Format: ANSWER: [your answer based on working memory]
                """
                
        return prompt
    
    def _find_node_by_id(self, current_node, node_id):
        """Find a node by ID in the tree."""
        if current_node["id"] == node_id:
            return current_node
        
        if "children" in current_node:
            for child in current_node["children"]:
                if child["id"] == node_id:
                    return child
        
        return None
    
    def process_query(self, text, query):
        """Process a query on a text document."""
        # First build the tree if not already built
        if not self.tree:
            self.build_tree(text)
        
        # Then navigate the tree to find the answer
        return self.navigate_tree(query)

# Example usage
if __name__ == "__main__":
    api_key = "your_openai_api_key"
    
    # Initialize the system
    mem_walker = MemWalkerSystem(api_key)
    
    # Example document text (would be much longer in practice)
    document = """
    [Long document text would go here...]
    """
    
    # Example query
    query = "What are the key financial metrics mentioned in the document?"
    
    # Process the query
    answer = mem_walker.process_query(document, query)
    print(f"Answer to query: {answer}")
