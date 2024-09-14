"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.8
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import preprocess_companies, preprocess_shuttles

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        # 1st node: preprocessing companies.csv
        node(
            func = preprocess_companies,
            inputs = "companies",
            outputs = "preprocessed_companies",
            name = "preprocess_companies_node"
        ),
        
        # 2nd node: preprocessing shuttles.xlsx
        node(
            func = preprocess_shuttles,
            inputs = "shuttles",
            outputs = "preprocessed_shuttles",
            name = "preprocess_shuttles_node"
        ),
    ])    