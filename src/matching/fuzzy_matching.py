from difflib import SequenceMatcher

def similarity_score(value1, value2):
    """
    Return a similarity score between 0 and 100.
    """
    if not value1 or not value2:
        return 0

    value1 = value1.lower().strip()
    value2 = value2.lower().strip()

    return round(
        SequenceMatcher(None, value1, value2).ratio() * 100, 2
    )
if __name__ == "__main__":

    print(
        similarity_score(
            "John Smith",
            "Jon Smith"
        )
    )

    print(
        similarity_score(
            "John Smith",
            "J. Smith"
        )
    )

    print(
        similarity_score(
            "John Smith",
            "Completely Different Person"
        )
    )