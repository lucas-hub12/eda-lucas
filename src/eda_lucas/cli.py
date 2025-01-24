from president_speech.db.parquet_interpreter import get_parquet_full_path
import pandas as pd
import typer
from tqdm import tqdm
import time

def add_keyword_count(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    """
    DataFrame에 keyword_count 컬럼을 추가하여 반환합니다.
    각 speech_text에서 keyword가 등장하는 횟수를 계산합니다.
    """
    # keyword_count 컬럼 추가
    df['keyword_count'] = df['speech_text'].str.count(keyword)
    return df

def group_by_count(keyword: str, asc: bool=False, rcnt: int=12, keyword_sum: bool=False):
    data_path = get_parquet_full_path()
    df = pd.read_parquet(data_path)
    fdf = df[df['speech_text'].str.contains(keyword, case=False)]

    if(keyword_sum):
        fdf = add_keyword_count(fdf.copy(), keyword)
        gdf = fdf.groupby("president").agg(
            count=("speech_text", "size"),  # 연설 개수
            keyword_sum=("keyword_count", "sum")  # keyword 발생 횟수 합산
        )
        sdf = gdf.sort_values(by=["keyword_sum", "count"], ascending=[asc, asc]).reset_index()
    else:
        gdf = fdf.groupby("president").size().reset_index(name="count")
        sdf = gdf.sort_values(by='count', ascending=asc).reset_index(drop=True)
    
    rdf = sdf.head(rcnt)
    return rdf

def print_group_by_count(keyword: str, asc: bool=False, rcnt: int=12, keyword_sum: bool=False):
    df = group_by_count(keyword, asc, rcnt, keyword_sum)
    # 프로그래스바 추가 - df 의 칼럼숫자 * row숫자 + sleep
    r = len(df.columns)*len(df)
    for i in tqdm(range(r)):
        time.sleep(0.1)
    
    from tabulate import tabulate
    hs = ["president", "count"]
    if keyword_sum:
       hs.append('keyword_sum') 
    t = tabulate(df,headers=hs, tablefmt ='rounded_outline', showindex=False)
     # t = tabulate(df,headers=["president", "count", "keyword_sum"], tablefmt ='rounded_outline')
    
   # else:
       # t = tabulate(df,headers=["president", "count"], tablefmt ='rounded_outline')
    
    print(t)
    # print(df.to_string(index=False))
    # print(tabulate(df))

def entry_point():
    typer.run(print_group_by_count)


