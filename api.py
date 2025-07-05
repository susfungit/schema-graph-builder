"""
High-level API for Schema Graph Builder

This module provides a clean, object-oriented interface for using the schema graph builder
programmatically without having to deal with individual modules.
"""

import os
import yaml
from typing import Dict, Any, Optional
from extractor.schema_extractor import extract_schema
from inference.relationship_inference import infer_relationships
from graph.graph_builder import build_graph


class SchemaGraphBuilder:
    """
    High-level interface for database schema analysis and visualization.
    
    This class provides a simple API for extracting database schemas,
    inferring relationships, and creating graph visualizations.
    
    Example:
        >>> builder = SchemaGraphBuilder()
        >>> result = builder.analyze_database(
        ...     db_type="postgres",
        ...     config_path="config/db_connections.yaml"
        ... )
        >>> print(f"Found {len(result['schema']['tables'])} tables")
    """
    
    def __init__(self):
        """Initialize the SchemaGraphBuilder."""
        self.last_schema = None
        self.last_relationships = None
    
    def analyze_database(
        self,
        db_type: str,
        config_path: str,
        output_dir: str = "output",
        visualize: bool = True,
        save_files: bool = True
    ) -> Dict[str, Any]:
        """
        Perform complete schema analysis for a database.
        
        Args:
            db_type: Database type ('postgres', 'mysql', 'mssql')
            config_path: Path to database configuration YAML file
            output_dir: Directory to save output files
            visualize: Whether to create HTML visualization
            save_files: Whether to save YAML and JSON files
            
        Returns:
            Dictionary containing:
            - schema: Extracted schema information
            - relationships: Inferred relationships
            - output_files: Paths to generated files
            
        Raises:
            ValueError: If database type is not supported
            FileNotFoundError: If config file doesn't exist
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        # Step 1: Extract schema
        schema = extract_schema(db_type, config_path)
        self.last_schema = schema
        
        # Step 2: Infer relationships
        relationships = infer_relationships(schema)
        self.last_relationships = relationships
        
        result = {
            "schema": schema,
            "relationships": relationships,
            "output_files": {}
        }
        
        if save_files:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Save relationships YAML
            relationships_file = os.path.join(output_dir, f"{db_type}_inferred_relationships.yaml")
            with open(relationships_file, "w") as f:
                yaml.dump(relationships, f, default_flow_style=False)
            result["output_files"]["relationships_yaml"] = relationships_file
            
            # Save and optionally visualize graph
            json_file = os.path.join(output_dir, f"{db_type}_schema_graph.json")
            build_graph(schema, relationships_file, visualize=visualize, output_json_path=json_file)
            result["output_files"]["graph_json"] = json_file
            
            if visualize:
                html_file = f"{db_type}_schema_graph.html"
                result["output_files"]["visualization_html"] = html_file
        
        return result
    
    def extract_schema_only(self, db_type: str, config_path: str) -> Dict[str, Any]:
        """
        Extract only the database schema without relationship inference.
        
        Args:
            db_type: Database type ('postgres', 'mysql', 'mssql')
            config_path: Path to database configuration YAML file
            
        Returns:
            Dictionary containing schema information
        """
        schema = extract_schema(db_type, config_path)
        self.last_schema = schema
        return schema
    
    def infer_relationships_only(self, schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Infer relationships from a schema.
        
        Args:
            schema: Schema dictionary (uses last extracted schema if None)
            
        Returns:
            Dictionary containing inferred relationships
            
        Raises:
            ValueError: If no schema is provided and none was previously extracted
        """
        if schema is None:
            if self.last_schema is None:
                raise ValueError("No schema provided and no previous schema available")
            schema = self.last_schema
            
        relationships = infer_relationships(schema)
        self.last_relationships = relationships
        return relationships
    
    def create_visualization(
        self,
        schema: Optional[Dict[str, Any]] = None,
        relationships: Optional[Dict[str, Any]] = None,
        output_path: str = "schema_graph.html"
    ) -> str:
        """
        Create an HTML visualization of the schema graph.
        
        Args:
            schema: Schema dictionary (uses last extracted if None)
            relationships: Relationships dictionary (uses last inferred if None)
            output_path: Path for the HTML file
            
        Returns:
            Path to the created HTML file
            
        Raises:
            ValueError: If required data is not available
        """
        if schema is None:
            if self.last_schema is None:
                raise ValueError("No schema provided and no previous schema available")
            schema = self.last_schema
            
        if relationships is None:
            if self.last_relationships is None:
                raise ValueError("No relationships provided and no previous relationships available")
            relationships = self.last_relationships
        
        # Create temporary YAML file for relationships
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(relationships, tmp, default_flow_style=False)
            tmp_path = tmp.name
        
        try:
            # Create visualization
            json_path = output_path.replace('.html', '.json')
            build_graph(schema, tmp_path, visualize=True, output_json_path=json_path)
            return output_path
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path) 