from extract import extracting
from transform import transforming
from load import loading


if __name__== "__main__" :

    data = extracting()
    cleaned_data = transforming(data)
    loading(cleaned_data)