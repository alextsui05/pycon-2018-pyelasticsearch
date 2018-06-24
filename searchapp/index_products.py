from elasticsearch import Elasticsearch
import elasticsearch.helpers

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {
                DOC_TYPE: {                                # This mapping applies to products.
                    'properties': {                        # Just a magic word.
                        'name': {                          # The field we want to configure.
                            'type': 'text',                # The kind of data we’re working with.
                            'fields': {                    # create an analyzed field.
                                'english_analyzed': {      # Name that field `name.english_analyzed`.
                                    'type': 'text',        # It’s also text.
                                    'analyzer': 'english', # And here’s the analyzer we want to use.
                                    }
                                }
                            }
                        }
                    }
            },
            'settings': {},
        },
    )

    # Index products one by one
    #for product in all_products():
    #    index_product(es, product)

    # Index products in bulk
    elasticsearch.helpers.bulk(es, actions_from_products())


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "image": product.image,
        }
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format(product.name))

def actions_from_products():
    for product in all_products():
        yield {
                '_op_type': 'index',
                '_index': INDEX_NAME,
                '_type': DOC_TYPE,
                '_id': product.id,
                '_source': {
                    "name": product.name,
                    "image": product.image
                    }
                }

if __name__ == '__main__':
    main()
