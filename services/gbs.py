from config.config import get_config
from requests import request
import textwrap

class GoogleBooksService():
    def __init__(self):
        print("Instantiating GoogleBooksService")
        self.link = "https://www.googleapis.com/books/v1/volumes"
    def getBookDataByISBN(self, isbn):
        print("ISBN:",isbn, self.link)
        gcp_config = get_config()["gcp"]
        link = self.link + "?q=isbn:{}&key={}".format(isbn, gcp_config["gb_api_key"])
        print("fetching data from link:", link)
        res = request("GET", link)
        resJSON = res.json()
        print("resJSON", resJSON)
        if ("items" not in resJSON or len(resJSON["items"]) == 0):
            print("no 'items' key in JSON res:", resJSON.keys())
            return {}
        fetched_book_data = resJSON["items"][0] 
        print("fetched_book_data:", fetched_book_data)
        sale_info = {}
        search_info = {}
        volume_info = {}
        if ("saleInfo" in fetched_book_data):
            sale_info = fetched_book_data["saleInfo"]
        if ("searchInfo" in fetched_book_data):
            search_info = fetched_book_data["searchInfo"]
        if ("volumeInfo" in fetched_book_data):
            volume_info = fetched_book_data["volumeInfo"]

        print("sale_info:", sale_info)
        print("search_info:", search_info)
        print("volume_info:", volume_info)

        return {
            "ISBN": isbn,
            "saleInfo": sale_info,
            "searchInfo": search_info,
            "volumeInfo": volume_info,
        }