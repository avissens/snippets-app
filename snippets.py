import psycopg2
import argparse
import logging

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    with connection, connection.cursor() as cursor:
        try:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
        connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet
    
def get(name):
    """Retrieve the snippet with a given name."""
    logging.info("Retrieving snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        command = "select message from snippets where keyword=%s"
        cursor.execute(command, (name,))
        row = cursor.fetchone()
    logging.debug("Snippet retrieved successfully.")
    if not row:
        # No snippet was found with that name.
        return "404: Snippet Not Found"
    return row[0]
    
def get_name(snippet):
    """Retrieve the snippet with a given message."""
    logging.info("Retrieving name {!r}".format(snippet))
    cursor = connection.cursor()
    command = "select keyword from snippets where message=%s"
    cursor.execute(command, (snippet,))
    row = cursor.fetchone()
    connection.commit()
    logging.debug("Name retrieved successfully.")
    if not row:
        # No snippet was found with that message.
        return "404: Name Not Found"
    return row[0]
    
def search(term):
    """Search the snippet by a given term."""
    logging.info("Searching snippets with %{!r}%".format(term))
    with connection, connection.cursor() as cursor:
        command = "select keyword from snippets where message like %s"
        cursor.execute(command, (term,))
        rows = cursor.fetchall()
        for message in rows:
            print(message[0])
    logging.debug("Search successfull.")
    if not rows:
        # No message was found with that term.
        return "404: Search returned 0 messages"
    
def catalog():
    """Query the keywords from the snippets table."""
    logging.info("Quering the keywords")
    with connection, connection.cursor() as cursor:
        command = "select keyword from snippets order by keyword"
        cursor.execute(command)
        rows = cursor.fetchall()
        for keyword in rows:
            print(keyword[0])
    logging.debug("Query successfull.")
    if not rows:
        # No keywords was found.
        return "404: No Keywords Found"

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")
    
    # Subparser for the get_name command
    logging.debug("Constructing get_name subparser")
    get_name_parser = subparsers.add_parser("get_name", help="Retrieve a name")
    get_name_parser.add_argument("snippet", help="Snippet text")
    
    # Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Searching snippets")
    search_parser.add_argument("term", help="Term in a snippet text")
    
    # Subparser for the cataloge command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="Catalog all keywords")
    
    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "get_name":
        name = get_name(**arguments)
        print("Retrieved name: {!r}".format(name))
    elif command == "search":
        snippet = search(**arguments)
        print("Snippets found: {!r}".format(snippet))
    elif command == "catalog":
        print("Catalog of all keywords:")
        catalog()

if __name__ == "__main__":
    main()