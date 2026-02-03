import streamlit as st
from snowflake.snowpark.functions import col

# Título de la App
st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# Input del nombre
name_on_order = st.text_input('Name on Smoothie:')

# Conexión (Este comando detecta automáticamente si estás en Snowflake o en Local con Secrets)
if 'snowflake' in st.connection:
    cnx = st.connection("snowflake")
    session = cnx.session()
else:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()

# Obtener lista de frutas
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

# Selector de ingredientes
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selections=5
)

if ingredients_list:
    # Creamos la cadena de texto separada por espacios para el HASH
    ingredients_string = ''
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
    
    # Quitamos el último espacio sobrante para que el HASH sea perfecto
    ingredients_string = ingredients_string.strip()

    # Construimos el INSERT
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders(ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    time_to_insert = st.button('Submit Order')

    if time_to_insert and name_on_order:
        session.sql(my_insert_stmt).collect()
        st.success(f'✅ Your Smoothie is ordered, {name_on_order}!')
