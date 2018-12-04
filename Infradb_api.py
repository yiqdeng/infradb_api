import logging
import requests
import yaml
from jinja2 import Template
import enum
import base64
# This infradb_api module is used to feed data to table "table_pub_node" in Hasura for teams
# created by yiqdeng
# update time: 2018.12.4

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

Transaction_Status_Template = Template('''
mutation updateTaskStatus{
update_table_pub_taskstatus(
where:{owner:{_eq:"{{ owner }}"}},
_set:{status_start:{{ status_start }},status_end:{{ status_end }}}
){
affected_rows
returning{
starttime
}
}
}''')


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
    def __init__(self,username,password,url,owner):
        self.username = username
        self.password = password
        self.url = url
        self.owner = owner

    def _post_graphql(self, query):
        account = ACCOUNT_Template.render(username=self.username, password=self.password)
        authorization = base64.b64encode(bytes(account, encoding='utf-8'))
        hasura_post_headers = {"Authorization": authorization,
                               "Content-Type": "application/json"}
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
        # add  decorate here  to check status

    def _trunk(self, listTemp, n):
        for i in range(0, len(listTemp), n):
            yield listTemp[i:i + n]

    def upsert_node_yaml(self,filepath,length):
        with open(filepath) as stream:
            try:
                parsed_yaml = yaml.load(stream)
                temp = self._trunk(parsed_yaml,length)
                for item in temp:
                    datalist = ''
                    for node in item:
                        xpath = node[YAMLKeys.NODE_XPATH_KEY.value]
                        type = node[YAMLKeys.NODE_TYPE_KEY.value]
                        attr = node[YAMLKeys.NODE_ATTR_KEY.value] if YAMLKeys.NODE_ATTR_KEY.value in node else ""
                        value = node[YAMLKeys.NODE_VALUE_KEY.value] if YAMLKeys.NODE_VALUE_KEY.value in node else ""
                        listindex = node[YAMLKeys.NODE_LISTINDEX_KEY.value]
                        owner = node[YAMLKeys.NODE_OWNER_KEY.value]
                        dataitem = DataItem_Template.render(xpath=xpath,type=type,attr=attr,value=value,listindex=listindex,
                                                            owner=owner)
                        datalist = dataitem + datalist

                    self._upsert_item(DataList=datalist)
            except Exception as err:
                LOGGER.error(err)
                query = Transaction_Status_Template(owner=self.owner, status_start="false", status_end="false")
                r = self._post_graphql(query=query)
                LOGGER.debug(r)
                return r.json()

    def query_nodes(self,graphql_query):
        r = self._post_graphql(query=graphql_query)
        LOGGER.debug(r)
        return r.json()

    def start_transaction(self):
        query = Transaction_Status_Template.render(owner=self.owner, status_start="true", status_end="false")
        r = self._post_graphql(query=query)
        LOGGER.debug(r)
        return r.json()

    def end_transaction(self):
        query = Transaction_Status_Template.render(owner=self.owner, status_start="false", status_end="true")
        r = self._post_graphql(query=query)
        LOGGER.debug(r)
        return r.json()

    def upsert_node_list(self, pylist, length):
            try:
                temp = self._trunk(pylist, length)

                for item in temp:
                    datalist = ''
                    for node in item:
                        xpath = node[YAMLKeys.NODE_XPATH_KEY.value]
                        type = node[YAMLKeys.NODE_TYPE_KEY.value]
                        attr = node[YAMLKeys.NODE_ATTR_KEY.value] if YAMLKeys.NODE_ATTR_KEY.value in node else ""
                        value = node[YAMLKeys.NODE_VALUE_KEY.value] if YAMLKeys.NODE_VALUE_KEY.value in node else ""
                        listindex = node[YAMLKeys.NODE_LISTINDEX_KEY.value]
                        owner = node[YAMLKeys.NODE_OWNER_KEY.value]
                        dataitem = DataItem_Template.render(xpath=xpath,type=type,attr=attr,value=value,
                                                            listindex=listindex, owner=owner)
                        datalist = dataitem + datalist
                    self._upsert_item(DataList= datalist)
            except Exception as err:
                LOGGER.error(err)
                query = Transaction_Status_Template.render(owner=self.owner, status_start="false", status_end="false")
                r = self._post_graphql(query=query)
                LOGGER.debug(r)
                return r.json()


# if __name__ == '__main__':
# Initial the Class
#     session = Session(username="hasura-dco.gen", password="xxxxxxx",
#                       url='https://csg-hasura-stage.webex.com/v1alpha1/graphql',owner="gen.dco")
#
#     pylist = [{'xpath': '/AMER', 'type': 'NT_ST_NETAPP_VFILER', 'attr': 'test', 'value': 'test2', 'listindex': 0,
#       'owner': 'gen.dco'},
#      {'xpath': '/AMER/SJC01', 'type': 'NT_ST_NETAPP_VFILER', 'attr': 'test', 'value': 'test1', 'listindex': 0,
#       'owner': 'gen.dco'},
#      {'xpath': '/AMER/SJC02', 'type': 'NT_ST_NETAPP_VFILER', 'attr': 'test', 'value': 'test3', 'listindex': 0,
#       'owner': 'gen.dco'}]
# Import data from python list,length must < 10000
#     session.start_transaction()
#     session.upsert_node_list(pylist=pylist,length=8000)
#     session.end_transaction()

# Import data  from  yaml file,length must < 10000
#     session.start_transaction()
#     session.upsert_node_yaml(filepath="......",length=8000)
#     session.end_transaction()




