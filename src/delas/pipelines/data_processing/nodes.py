"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.8
"""

import pandas as pd

def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    # convert iata_approved column to bool
    companies['iata_approved'] = companies['iata_approved'].map({'t': True, 'f': False})

    # convert company_rating to float
    companies['company_rating'] = pd.to_numeric(companies['company_rating'], errors = 'coerce')
    
    return companies


def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    # convert d_check_complete column to bool
    shuttles['d_check_complete'] = shuttles['d_check_complete'].map({'t': True, 'f': False})

    # convert moon_clearance_complete column to bool
    shuttles['moon_clearance_complete'] = shuttles['moon_clearance_complete'].map({'t': True, 'f': False})

    # convert price to float
    shuttles['price'] = pd.to_numeric(shuttles['price'], errors = 'coerce')

    return shuttles