package DataFrames

case class Order_items(
      order_item_id : Int,
      order_item_order_id : Int,
      order_item_product_id : Int,
      order_item_quantity : Int,
      order_item_subtotal: Float,
      order_item_price : Float
)