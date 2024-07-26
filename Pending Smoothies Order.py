# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("""Orders that need to be filled.""")

# Get active session
session = get_active_session()

# Load data from the Snowflake table
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False).to_pandas()

if my_dataframe:

# Use Streamlit's data editor to display the dataframe
    editable_df = st.experimental_data_editor(my_dataframe)

# Add a Submit button
    submitted = st.button('Submit')

    if submitted:
    
        og_dataset = session.table("smoothies.public.orders")
        edited_dataframe = session.create_dataframe(editable_df)
            
        try:
            og_dataset.merge(edited_dataframe,
                                 (og_dataset['order_uid'] == edited_dataframe['order_uid']),
                                 [when_matched().update({"ORDER_FILLED": edited_dataframe['ORDER_FILLED']})]
                                 )
            st.success("Order(s) Updated!", icon="👍")
        except:
            st.write(f'Something went wrong.')
else:
    st.success('There are no pending orders right now', icon="👍")
   










