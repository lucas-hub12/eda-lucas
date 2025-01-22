from eda_lucas.cli import group_by_count  
import pandas as pd

def test_search_exception():
    row_count = 13
    df = group_by_count(keyword="자유", asc=True, rcnt=row_count)
    
    # assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) < row_count

def test_정열_및_행수제한():
    # given
    row_count = 3
    is_asc = True

    # when
    df = group_by_count(keyword="자유", asc=is_asc, rcnt=row_count)
    
    # then
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["president"] == "윤보선"
    assert len(df) == row_count

def test_default_args():
    # given

    # when
    df = group_by_count(keyword="자유")
    
    # then
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 12
    assert df.iloc[0]["president"] == "박정희" 
    assert df.iloc[0]["count"] == 513
    assert df.iloc[1]["count"] == 438
    assert df.iloc[11]["count"] == 1

presidents_speeches = {
    "박정희": 513,
    "이승만": 438,
    "노태우": 399,
    "김대중": 305,
    "문재인": 275,
    "김영삼": 274,
    "이명박": 262,
    "전두환": 242,
    "노무현": 230,
    "박근혜": 111,
    "최규하": 14,
    "윤보선": 1
}

def test_all_count():
    # given
    # global dict
    # when
    df = group_by_count(keyword="자유")
    
    # then
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 12
    for p_name, s_count in presidents_speeches.items():
        president_row = df[df["president"]== p_name]
        assert president_row.iloc[0]["count"] == s_count

