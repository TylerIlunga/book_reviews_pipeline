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
        res = request("GET", link)
        resJSON = res.json()
        # print("book_data", book_data)
        book_data = resJSON["items"][0] 
        # print("book_data:", book_data)
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
            "saleInfo": book_data["saleInfo"],
            "searchInfo": book_data["searchInfo"],
            "volumeInfo": book_data["volumeInfo"],
        }

