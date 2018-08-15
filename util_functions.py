
# coding: utf-8

# In[34]:


# Various utility functions
def get_dataset_ids(client_name):
    dataset_list = list(client_name.list_datasets())
    
    if dataset_list:
        dataset_ids = list()
        print(f'Datasets in project - {client_name.project}:')
        for dataset in dataset_list:
            print(dataset.dataset_id)
            dataset_ids.append(dataset.dataset_id)
            
        return dataset_ids

    else:
        print(f'No datasets in {client_name.project}')        

def get_table_ids(client_name, dataset):

    table_list = list(client_name.list_tables(dataset))

    if table_list:
        table_ids = list()
        print(f'Tables in {dataset.dataset_id}:')
        for table in table_list:
            print(f'{table.table_id}')
            table_ids.append(table.table_id)

        return table_ids

    else:
        print(f'No tables in {dataset.dataset_id}')
        
def get_dataset(client_name, dataset_id, table_ids=False):
    dataset_ref = client_name.dataset(dataset_id)
    
    dataset = client_name.get_dataset(dataset_ref)
    
    if dataset:
        print('---------------------------------------')
        print(f'Dataset ID: {dataset.dataset_id}')
        print(f'Friendly Name: {dataset.friendly_name}')
        print(f'Full ID: {dataset.full_dataset_id}')
        print(f'Labels: {dataset.labels}')
        print(f'Project: {dataset.project}')
        print(f'Ref: {dataset.reference}')

        if table_ids == True:
            table_ids = get_table_ids(client_name, dataset)
            
            return dataset, table_ids

        print('---------------------------------------')
    
    else:
        print(f'No dataset matching dataset_id {dataset_id} in project {client_name.project}')

    return dataset

def get_table(client_name, dataset, table_id, incl_schema=False):
    table_ref = dataset.table(table_id)
    
    table = client_name.get_table(table_ref)
    
    if table:
        print('---------------------------------------')
        print(f'Table ID: {table.table_id}')
        print(f'Friendly Name: {table.friendly_name}')
        print(f'Full ID: {table.full_table_id}')
        print(f'Type: {table.table_type}')
        print(f'Rows: {table.num_rows}')
        if incl_schema == True:
            print(f'\nSchema:\n{table.schema}') 
    
    else:
        print(f'{table_id} not present in dataset {dataset}')
    
    return table

def create_dataset(client_name, dataset_id, dataset_location=None, project=None):
    
    # Creates dataset reference for bq
    dataset_reference = client_name.dataset(dataset_id, project)
    
    # Creates actual data set object
    dataset = bq.Dataset(dataset_reference)
    
    # Optionally sets dataset location value
    if dataset_location:
        dataset.location = dataset_location
    
    # Makes the call home
    dataset = client_name.create_dataset(dataset)
    
    return f'{dataset_id} created in project - {client_name.project}'

