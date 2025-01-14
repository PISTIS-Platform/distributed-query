# Distributed Query

The main purpose of this component is to query directly the unstructured or 
semi-structured data to discover datasets that cannot be retrieved by 
querying their metadata on the Distributed Data Catalogue.  

However, the volume of the data stored in the Data Factories does not allow
extensive search approaches to be used. Therefore, Locality Sensitive Hashing 
techniques are employed to quickly obtain a list of matches.