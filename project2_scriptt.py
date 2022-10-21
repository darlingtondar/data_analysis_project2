#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re


# In[2]:


pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.max_rows', 1000)

orders = pd.read_csv('orders.csv')

orders.duplicated().sum()

orders.info()
#reveal missing values

print(f"5 missing values represents {((orders.total_paid.isna().sum() / orders.shape[0])*100).round(5)}% of the rows in our DataFrame")
#orderlines= pd.read_csv('orderlines.csv')

orders = orders.loc[~orders.total_paid.isna(), :] #missing value delected

#orders.info()

#created_date should become datetime datatype

orders["created_date"] = pd.to_datetime(orders["created_date"])

orders["month"] = orders["created_date"].dt.month
orders["year"] = orders["created_date"].dt.year
orders["day"] = orders["created_date"].dt.day

orders[orders.state.isin(['Completed'])]
#orders["orders_completed"] =orders[orders.state.isin(['Completed'])]


#orders= orders.assign(orders_completed[orders.state.isin(['Completed'])])
    

orders.info()


# In[3]:


orderlines = pd.read_csv('orderlines.csv')

 
#orderlines.info()  #No data missing data but date datatpe should be change to datetime and price should be float

orderlines["date"] = pd.to_datetime(orderlines["date"])

#orderlines.info()

#orderlines["unit_price"] = pd.to_numeric(orderlines["unit_price"]) 

#As you can see when we try to convert unit_price to
#a numerical datatype, we receive a ValueError telling us that pandas 
#doesn't understand the number 1.137.99. This is probably because numbers 
#cannot have 2 decimal points. Let's see if there are any other numbers like this.

orderlines.unit_price.astype(str).str.contains("\d+\.\d+\.\d+").value_counts()

#Looks like over 36000 rows in orderlines are affected by this problem.
#Let's work out how much that is as a percentage of our total data.

two_dot_percentage = ((orderlines.unit_price.astype(str).str.contains("\d+\.\d+\.\d+").value_counts()[1] / orderlines.shape[0])*100).round(2)

print(f"The 2 dot problem represents {two_dot_percentage}% of the rows in our DataFrame")

two_dot_order_ids_list = orderlines.loc[orderlines.unit_price.astype(str).str.contains("\d+\.\d+\.\d+"), "id_order"]

orderlines = orderlines.loc[~orderlines.id_order.isin(two_dot_order_ids_list)]

#orderlines.info()

#Now that all of the 2 decimal point prices have been removed, 
#let's try again to convert the column unit_price to the correct datatype.

orderlines["unit_price"] = pd.to_numeric(orderlines["unit_price"]) 

orderlines["month"] = orderlines["date"].dt.month
orderlines["year"] = orderlines["date"].dt.year
orderlines["day"] = orderlines["date"].dt.day


# In[4]:


orders_completed = orders.drop(orders[orders.state.isin(["Cancelled","Shopping Basket", "Pending", "Place Order"])].index)

orders_completed.head()


# In[5]:


orders_completed.info()


# In[6]:


########################################################change the name of id_order column in orderline to merge with orders
orderlines.rename(columns = {'id_order':'order_id'},inplace = True)

orderlines.info()


# In[7]:


#####merge two datafram to have same order_1d
merge_ids =orderlines.merge(orders_completed, how="inner", on ="order_id")
merge_ids.info()


# In[8]:


################Missing values
#dec  7, 
#pric 46 - 0.23802% 
#type 50 missing data - 0.25872%

#%of missing data to overall data

#print(f"50 missing values represent {((products.type.isna().sum() / products.shape[0])*100).round(5)}% of the rows in our DataFrame")

#As there is such a tiny amount of missing values, we will simply delete these rows, as we have enough data without them.
products = pd.read_csv('products.csv')

products = products.loc[~products.desc.isna(), :]

products = products.loc[~products.price.isna(), :]

products = products.loc[~products.type.isna(), :]

products.info()


# In[9]:


#Check / Change Data types####################
#######################

#products["price"] = pd.to_numeric(products["price"])
#We saw from looking at the output of `.info()` that both `price` and `promo_price` have been stored 
#as objects and not as a numerical datatypes. We also saw while solving other problems that both columns 
#have some prices with 3 decimal places and others with 2 decimal points - the latter will prevent us
#from converting the datatype to numerical, so first we must investigate and solve these problems.

#products["price"] = pd.to_numeric(products["price"])

price_problems_number = products.loc[(products.price.astype(str).str.contains("\d+\.\d+\.\d+"))|(products.price.astype(str).str.contains("\d+\.\d{3,}")), :].shape[0]
#price_problems_number

#print(f"The column price has in total {price_problems_number} wrong values. This is {round(((price_problems_number / products.shape[0]) * 100), 2)}% of the rows of the DataFrame")

products = products.loc[(~products.price.astype(str).str.contains("\d+\.\d+\.\d+"))&(~products.price.astype(str).str.contains("\d+\.\d{3,}")), :]

products["price"] = pd.to_numeric(products["price"])
products.info()

################################promo type


# In[10]:


################################################promo_price

promo_problems_number = products.loc[(products.promo_price.astype(str).str.contains("\d+\.\d+\.\d+"))|(products.promo_price.astype(str).str.contains("\d+\.\d{3,}")), :].shape[0]
promo_problems_number


# In[11]:


print(f"The column price has in total {promo_problems_number} wrong values. This is {round(((promo_problems_number / products.shape[0]) * 100), 2)}% of the rows of the DataFrame")


# In[12]:


promo_price_df = products.loc[(products.promo_price.astype(str).str.contains("\d+\.\d+\.\d+"))|(products.promo_price.astype(str).str.contains("\d+\.\d{3,}")), :]
promo_price_df.sample()


# In[13]:


products_c = products.drop(columns=["promo_price"])

products_c.drop_duplicates().sum


# In[14]:


products_c.drop_duplicates()
products_cc= products_c.drop_duplicates()


# In[15]:


products_cc.info()


# In[16]:


#Exclude orders with unknown products

products_sku =products.sku

merge_ids_sku = merge_ids[merge_ids["sku"].isin(products_sku)]
merge_ids_sku.info()


# In[17]:


#4. Explore the revenue from different tables

merge_ids['unit_price_total']=merge_ids.unit_price * merge_ids.product_quantity  
merge_ids.head(5)


# In[18]:


merge_ids.info()


# In[19]:


merge_ids.groupby(["order_id"])["unit_price_total"].sum()


# In[20]:


merge_ids_short = merge_ids.groupby(["order_id"])["unit_price_total"].sum()
merge_ids_short.head(5)


# In[21]:


merge_ids.head(5)


# In[22]:


diff_merg = orders_completed.merge(merge_ids_short, left_on="order_id", right_on="order_id")


# In[23]:


diff_merg.head()


# In[24]:


#merging of productsdf & orderdf using sku
#Discounts are defined as the difference between orderlines.unit_price and products.price
pro_dis = products_cc.merge(merge_ids, left_on="sku", right_on="sku")

pro_dis.head(7)


# In[25]:


pro_dis= pro_dis.drop(columns=["date","year_x","day_x","month_y","year_y","day_y"])
pro_dis.head(5)


# In[26]:


pro_dis["discount"]=  pro_dis["price"] - pro_dis["unit_price"]


# In[27]:


pro_dis.head(5)


# In[28]:


pro_dis["sku"].count()


# In[29]:


charging_categories = ["charger", "power bank", "battery", "powerhouse"]
storage = ["drive", "ssd", "hard disk", "xpand", "memory","sd"]
covers = ["case", "briefcase", "backpack", "sleeve", "cover"]
hardware = ["keyboard", "mouse", "adapter", "monitor", "cable"]
smartwatches = ["smartwatch", "clock", "apple watch"]
servers = ["server", "network"]
sound_accessories = ["speaker", "headphones", "airpods", "headsets", "headset"]
software_protection = ["applecare"]
tablets = ["tablet"]
phones = r"^.{0,11}apple ip"
protection = ["tempered glass", "screen protector"]
laptops = r"^.{0,6}ram"

def categorise(row):
    name = row["name"].lower()
    desc = row["desc"].lower()
    if any(x in name or x in desc for x in charging_categories):
        return "charging accessories"
    elif any(x in name or x in desc for x in smartwatches):
        return "Smart Watches"
    elif any(x in name or x in desc for x in storage):
        return "Storage"
    elif any(x in name or x in desc for x in covers):
        return "Covers"
    elif any(x in name or x in desc for x in hardware):
        return "hardware"
    elif any(x in name or x in desc for x in servers):
        return "servers"
    elif any(x in name or x in desc for x in sound_accessories):
        return "sound_accessories"
    elif any(x in name or x in desc for x in software_protection):
        return "software_protection"
    elif any(x in name or x in desc for x in tablets):
        return "tablets"
    elif any(x in name or x in desc for x in protection):
        return "protection"
    elif re.findall(phones, name) or re.findall(phones, desc):
        return "phones"
    elif re.findall(laptops,desc):
        return "laptops"
    else:
        return "others"



pro_dis["category"] = pro_dis.apply(categorise, axis=1)
pro_dis.head(2)


# In[30]:


product_category_df= pro_dis.copy()


# In[31]:


product_category_df = product_category_df.drop(columns=["sku","in_stock","id","product_id","month_x","desc", "type"])
product_category_df.head(5)


# In[32]:


product_category_df["date"] = product_category_df["created_date"].dt.strftime("%A, %d %b %y")
product_category_df.head(20)


# In[33]:


#What is the distribution of product prices across different categories

product_category_df.groupby(["category"])["unit_price"].agg(["sum", "count"]).nlargest(5, "sum")


# In[34]:


#total amont of products in each category
#How many products are being discounted
dfa=product_category_df.groupby(["category"])["product_quantity"].count().sort_values(ascending=False).nlargest(13)


# In[35]:


dfa #answer=55,304


# In[36]:


#How many products are being discounted
#product_category_df.groupby(["category"])["discount"!=0].count().nlargest(5)

df1 = product_category_df[product_category_df.discount!=0].groupby('category', as_index=False).discount.count()


# In[37]:


df1.count #answer=52988


# In[38]:


import seaborn as sns
import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt


# In[ ]:





# In[39]:


sns.displot(data=product_category_df,
            y="category", 
            stat="percent");


# In[40]:


#sns.catplot(
            #data=df1, 
            #y="category", 
            #x="discount",
            #color="red");

sns.catplot(kind="bar",
            data=df1,
            x="category",
            y="discount",
            height=8,
            aspect=2,
            palette="YlOrBr");


# In[41]:


#average discount in each category
#product_category_df.groupby(["category"])["discount"!=0].count().nlargest(5)

df2 = product_category_df[product_category_df.discount!=0].groupby('category', as_index=False).discount.mean()


# In[42]:


df2 


# In[43]:


#sns.catplot(
            #data=df2, 
            #y="category", 
            #x="discount",
            #color="red");

sns.catplot(kind="bar",
            data=df2,
            x="category",
            y="discount",
            height=7,
            aspect=2,
            palette="Blues");
sns.set_context("poster", font_scale = 1, rc={"grid.linewidth": 5})


# In[44]:


#How big are the offered discounts as a percentage of the product prices
product_category_df["discount"] * 100 / product_category_df["price"].round(2)


# In[45]:


#How seasonality and special dates (Christmas, Black Friday) affect sales
product_category_df.groupby(["date"])["total_paid"].agg(["sum", "count"])


# In[46]:


#How seasonality and special dates (Christmas, Black Friday) affect sales
product_category_df.groupby(["date"])["discount"].agg(["sum", "count"])


# In[47]:


#finding out whether offering discounts is beneficial for the company
product_category_df.groupby(["total_paid"])["discount"].agg(["sum"])


# In[ ]:





# In[ ]:




