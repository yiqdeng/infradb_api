import logging
import requests
import yaml
from jinja2 import Template
import enum
import base64


ACCOUNT_Template = Template('{{username}}:{{password}}')

NODE_UPSERT_TEMPLATE = Template('''
mutation upsert_node {
  insert_table_pub_node(objects:[{{ DataList }}], on_conflict: {constraint: node_complex_unique_constraint}) {
    affected_rows
    returning {
      type
    }
  }
}
''')

DataItem_Template = Template('''{ xpath: "{{ xpath }}", type: "{{ type }}", 
                            attr: "{{ attr }}",value: "{{ value }}",listindex:"{{ listindex }}",owner:"{{ owner }}"}''')


class YAMLKeys(enum.Enum):
    NODE_XPATH_KEY = "xpath"
    NODE_TYPE_KEY = "type"
    NODE_VALUE_KEY = "value"
    NODE_ATTR_KEY = "attr"
    NODE_OWNER_KEY = "owner"
    NODE_LISTINDEX_KEY = "listindex"


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler())
LOGGER.setLevel(logging.DEBUG)


class Session(object):
    def __init__(self,username,password,url):
        self.username = username
        self.password = password
        self.url = url

    def _post_graphql(self, query):
        account = ACCOUNT_Template.render(username=self.username, password=self.password)
        authorization = base64.b64encode(bytes(account, encoding='utf-8'))
        hasura_post_headers = {"Authorization": authorization,
                               "Content-Type":"application/json"}
        post_args = {
            'headers': hasura_post_headers,
            'json': {'query': query}
        }
        request = requests.post(self.url, **post_args)
        LOGGER.debug(request.request.body)
        LOGGER.debug(request.reason)
        request.raise_for_status()
        return request

    def _upsert_item(self, DataList):
        query = NODE_UPSERT_TEMPLATE.render(DataList=DataList)
        LOGGER.debug(query)
        self._post_graphql(query)

    def _trunk(self,listTemp, n):
        for i in range(0, len(listTemp), n):
            yield listTemp[i:i + n]

    def upsert_node(self,filepath):
        with open(filepath) as stream:
            try:
                parsed_yaml = yaml.load(stream)
                temp = self._trunk(parsed_yaml,10000)
                for item in temp:
                    DataList = ''
                    for node in item:
                        xpath = node[YAMLKeys.NODE_XPATH_KEY.value]
                        type = node[YAMLKeys.NODE_TYPE_KEY.value]
                        attr = node[YAMLKeys.NODE_ATTR_KEY.value] if YAMLKeys.NODE_ATTR_KEY.value in node else ""
                        value = node[YAMLKeys.NODE_VALUE_KEY.value] if YAMLKeys.NODE_VALUE_KEY.value in node else ""
                        listindex = node[YAMLKeys.NODE_LISTINDEX_KEY.value]
                        owner = node[YAMLKeys.NODE_OWNER_KEY.value]
                        DataItem = DataItem_Template.render(xpath=xpath,type=type,attr=attr,value=value,listindex=listindex,
                                                            owner=owner)
                        DataList = DataItem + DataList
                    self._upsert_item(DataList=DataList)
            except yaml.YAMLError as yerr:
                LOGGER.error("Unable to parse the schema yaml{}".format(yerr))

    def query_nodes(self,graphql_query):
        r = self._post_graphql(query=graphql_query)
        LOGGER.debug(r)
        return r.json()


# if __name__ == '__main__':
#     session = Session(username="hasura-dco.gen", password="cH4&p0W5t",
#                       url='https://csg-hasura-stage.webex.com/v1alpha1/graphql')
#     session.upsert_node(filepath=r"C:\Users\yiqdeng\infradb-schema-creator\PUBNODE")
#
#     query = '''query{
#   table_pub_taskstatus{
#     id
#     owner
#     status
#   }
# }'''
#     print(session.query_nodes(query))



