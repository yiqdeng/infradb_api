InfraDB schema creator
======================

A simple python tool that can be used to feed the schema data to InfraDB_ using GraphQL. This tool enables creation of the Node Type and Node Attributes entities in InfraDB. The information about the node type and it's attributes needs to be specified in a simple YAML format.

.. _InfraDB : https://wiki.cisco.com/display/CMSE/INFRA+DATA

Installation
------------

::

     git clone <This repo> infradb-schema-creator
     cd infradb-schema-creator
     python3.6 -m venv venv
     source venv/bin/activate
     pip install -r requirements.txt
     
     
Example Usage
-------------

::
    
    python infradb_schema_creator.py schemas/storage/7mode.yml 
     
