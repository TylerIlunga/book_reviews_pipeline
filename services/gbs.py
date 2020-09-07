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
        book_data = resJSON["items"][0] 
        # print("book_data:", book_data)
        sale_info = {}
        search_info = {}
        volume_info = {}
        if ("saleInfo" in book_data):
            sale_info = book_data["saleInfo"]
        if ("searchInfo" in book_data):
            search_info = book_data["searchInfo"]
        if ("volumeInfo" in book_data):
            volume_info = book_data["volumeInfo"]

        print("sale_info:", sale_info)
        print("search_info:", search_info)
        print("volume_info:", volume_info)

        # Strings with apostrophes need to have two single quotes instead of one
        # book_desc_update = ""
        # if "description" in book_data["volumeInfo"]:
        #     book_desc_update = book_data["volumeInfo"]["description"].replace("'", "''")
        # elif "textSnippet" in book_data["searchInfo"]:
        #     book_desc_update = book_data["searchInfo"]["textSnippet"].replace("'", "''")
        
        # book_data["volumeInfo"]["description"] = book_desc_update

        # authors = book_data["volumeInfo"]["authors"]
        # print("\nTitle:", book_data["volumeInfo"]["title"])
        # print("\nSummary:\n")
        # # print(textwrap.fill(book_data["searchInfo"]["textSnippet"], width=65))
        # print("\nAuthor(s):", ",".join(authors))
        # print("\nPublic Domain:", book_data["accessInfo"]["publicDomain"])
        # print("\nPage count:", book_data["volumeInfo"]["pageCount"])
        # print("\nLanguage:", book_data["volumeInfo"]["language"])
        # print("\n***")
        return {
            "ISBN": isbn,
            "saleInfo": sale_info,
            "searchInfo": search_info,
            "volumeInfo": volume_info,
        }



# GBS = GoogleBooksService()
# print("GBS res:", GBS.getBookDataByISBN(isbn="0030119537"))