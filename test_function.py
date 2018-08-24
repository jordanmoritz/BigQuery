
# coding: utf-8

# In[1]:


# To test queueing functions to the redis server
def count_words(url):
    resp = requests.get(url)
    return len(resp.text.split())

