for $p_id in distinct-values(doc("purchaseorders.xml")//PurchaseOrder/item/partid)

let $item_price:= doc("products.xml")//product[@pid=$p_id]

let $i_id:= doc("purchaseorders.xml")//PurchaseOrder/item[partid=$p_id]

order by $p_id
return <totalcost partid="{$p_id}">{$item_price/description/price*sum($i_id/quantity)}</totalcost>