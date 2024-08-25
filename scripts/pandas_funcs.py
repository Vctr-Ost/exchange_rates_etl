import pandas as pd

from logging import getLogger
logger = getLogger(__name__)


def create_df_from_dict(dict: dict, columns: list, orient: str) -> pd.DataFrame:
    """
    Формує pandas DataFrame із dict
    Якщо довжина параметра "columns" > 0 - перейменовує колонки на ті що вказані у "columns"

    Args:
        dict (dict): Dict із даними
        columns (list): List із назвами колонок
        orient (str): параметр orient для функції pd.DataFrame.from_dict. 
                      Наразі логіка є тільки для orient='index'
        
    Returns:
        pandas.DataFrame: Результуючий DataFrame із даними із dict
    """

    if orient == 'index':
        logger.info('Creating df from list .....')
        df = pd.DataFrame.from_dict(dict, orient='index').reset_index()
        logger.info('Df from list created')
        if len(columns) > 0:
            df.columns = columns
            logger.info('Df columns renamed')
        return df
    else:
        logger.error(f'Arg "orient" == {orient} - is unsuitable')
        raise ValueError("Arg 'orient' is unsuitable")


def add_column(df: pd.DataFrame, col_name: str, col_value) -> pd.DataFrame:
    """
    Додає колонку в pandas.DataFrame із константою

    Args:
        df (pd.DataFrame): pandas df до якого потрібно додати колонку
        col_name (str): назва нової колонки
        col_value (any): значення для нової колонки col_name

    Returns:
        pandas.DataFrame: Результуючий DataFrame із новою колонкою
    """
    
    logger.info(f'Adding the column {col_name}, value == {col_value} .....')
    df[col_name] = col_value
    logger.info(f'Column {col_name} added')
    return df

