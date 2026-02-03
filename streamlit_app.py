# --- SECCIÓN DE ADMINISTRACIÓN (Simplificada) ---
st.subheader("Pending Orders")

# Leemos la tabla y filtramos solo los que no están rellenos
# Quitamos el order_by temporalmente para ver si es el causante del error
try:
    pending_df = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False)
    
    # Comprobamos si hay datos de forma más segura
    data_check = pending_df.collect()
    
    if len(data_check) > 0:
        st.dataframe(pending_df)
        
        # Convertimos a Pandas para el selector
        pd_df = pending_df.to_pandas()
        order_to_fill = st.selectbox("Select Name to Mark as Filled:", pd_df["NAME_ON_ORDER"])
        
        if st.button("Mark as Filled"):
            # UPDATE exacto
            session.sql(f"UPDATE smoothies.public.orders SET ORDER_FILLED = TRUE WHERE NAME_ON_ORDER = '{order_to_fill}'").collect()
            st.success(f"Order for {order_to_fill} marked as filled!")
            st.rerun()
    else:
        st.write("No pending orders.")
except Exception as e:
    st.error("Error al leer la tabla. Asegúrate de que las columnas NAME_ON_ORDER y ORDER_FILLED existan.")
