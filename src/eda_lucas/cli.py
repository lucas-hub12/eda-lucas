from president_speech.db.parquet_interpreter import read_parquet, get_parquet_full_path
import pandas as pd
import typer

def group_by_count(keyword: str, ascending: bool = False):
    # todo:ascending, 출력 rows size
    # pytest 코드를 만들어보세요
    # import this <- 해석해보세요
    data_path = get_parquet_full_path()
    df = pd.read_parquet(data_path)
    f_df = df[df['speech_text'].str.contains(keyword, case=False)]
    rdf = f_df.groupby("president").size().reset_index(name="count")
    rdf.sort_values(by='count', ascending=False).reset_index(drop=True)
    sdf = rdf.sort_values(by='count', ascending=False).reset_index(drop=True)
    print(sdf.to_string(index=False))
    print(f"Rows size: {sdf.shape[0]}")

def entry_point():
    typer.run(group_by_count)

def test_addition():
    assert 1 + 1 == 2

def test_subtraction():
    assert 5 - 3 == 2
