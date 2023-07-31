<img src="https://i.ibb.co/5RM26Cw/LOGO-COLOR2.png" width="500px">

Clase 03 - Ejercicio SQL
========================

AVG
---

### Ejercicio 1

Obtener el promedio de precios por cada categoría de producto. La cláusula OVER(PARTITION BY CategoryID) específica que se debe calcular el promedio de precios por cada valor único de CategoryID en la tabla.

```sql
select
    c.category_name,
    p.product_name,
    p.unit_price
    avg(p.unit_price) over(partition by p.category_id) as avg_price_by_category
from
    products p
    left join categories c on p.category_id = c.category_id;
```

### Ejercicio 2

Obtener el promedio de venta de cadacliente.

```sql
select
    avg(od.unit_price * od.quantity) over (partition by customer_id) as avg_order_amount,
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date
from
    orders o
    inner join order_details od on o.order_id = od.order_id
order by
    o.customer_id;
```

### Ejercicio 3

Obtener el promedio de cantidad de productos vendidos por categoría (product_name, quantity_per_unit, unit_price, quantity, avgquantity) y ordenarlo por nombre de la categoría y nombre del producto.

```sql
select
    p.product_name,
    c.category_name,
    p.quantity_per_unit,
    od.unit_price,
    od.quantity,
    avg(od.quantity) over (partition by c.category_id) as avg_quantity
from
    products p
    inner join order_details od on p.product_id = od.product_id 
    left join categories c on p.category_id = c.category_id
order by
    c.category_name, p.product_name;
```

MIN
---

### Ejercicio 4

Selecciona el ID de lcliente, la fecha de la orden y la fecha más antigua de la orden para cada cliente de la tabla 'Orders'.

```sql
select
    o.customer_id,
    o.order_date,
    min(o.order_date) over (partition by o.customer_id) as earliest_order_date
from
    orders o;
```

MAX
---

### Ejercicio 5

Seleccione el id de producto, el nombre de producto, el precio unitario, el id de categoría y el precio unitario máximo para cada categoría de la tabla Products..

```sql
select
    p.product_id,
    p.product_name,
    p.unit_price,
    p.category_id,
    max(p.unit_price) over (partition by p.category_id) as max_unit_price
from
    products p;
```

ROW_NUMBER
----------

### Ejercicio 6

Obtener el ranking de los productos más vendidos.

```sql
select
    row_number() over (order by table1.total_quantity DESC) as ranking,
    table1.product_name,
    table1.total_quantity
from (
    select
        p.product_name,
        sum(od.quantity) as total_quantity
    from
        order_details od
        left join products p on od.product_id = p.product_id
    group by
        p.product_name
) as table1;
```

### Ejercicio 7

Asignar numeros de fila para cada cliente, ordenados por customer_id.

```sql
select
    row_number() over (order by c.customer_id) as rownumber,
    c.customer_id,
    c.company_name,
    c.contact_name,
    c.contact_title,
    c.address,
    c.city,
    c.region,
    c.postal_code,
    c.country,
    c.phone,
    c.fax
from
    customers c;
```

### Ejercicio 8

Obtener el ranking de los empleados más jóvenes (ranking, nombre y apellido del empleado, fecha de nacimiento).

```sql
select
    row_number() over (order by e.birth_date desc) as ranking,
    concat(e.first_name, ' ', e.last_name) as employee_name,
    e.birth_date
from
    employees e;
```

SUM
---

### Ejercicio 9

Obtener la suma de venta de cada cliente.

```sql
select
    sum(od.unit_price * od.quantity) over (partition by o.customer_id) as sum_order_amount,
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date
from
    orders o
    inner join order_details od on o.order_id = od.order_id;
```

### Ejercicio 10

Obtener la suma total de ventas por categoría de producto.

```sql
select
    c.category_name,
    p.product_name,
    od.unit_price,
    od.quantity,
    sum(od.unit_price * od.quantity) over (partition by c.category_id) as total_sales
from
    order_details od
    left join products p on od.product_id = p.product_id
    left join categories c on p.category_id = c.category_id
order by
    c.category_name, p.product_name;
```

### Ejercicio 11

Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país y por orden de manera ascendente.

```sql
select
    o.ship_country,
    o.order_id,
    o.shipped_date,
    o.freight,
    sum(o.freight) over (partition by o.ship_country) as total_shipping_costs
from
    orders o
order by
    o.ship_country asc, o.order_id asc;
```

RANK
----

### Ejercicio 12

Ranking de ventas por cliente.

```sql
select
    table1.customer_id,
    table1.company_name,
    table1.total_sales,
    rank() over (order by table1.total_sales desc) as ranking
from
    (
    select
        o.customer_id,
        c.company_name,
        sum(od.unit_price * od.quantity) as total_sales
    from
        orders o
        inner join order_details od on o.order_id = od.order_id
        left join customers c on o.customer_id = c.customer_id
    group by
        o.customer_id, c.company_name
) table1;
```

### Ejercicio 13

Ranking de empleados por fecha de contratacion.

```sql
select
    e.employee_id,
    e.first_name,
    e.last_name,
    e.hire_date,
    rank() over (order by e.hire_date) as ranking
from
    employees e;
```

### Ejercicio 14

Ranking de productos por precio unitario.

```sql
select
    p.product_id,
    p.product_name,
    p.unit_price,
    rank() over (order by p.unit_price desc) as ranking
from
    products p;
```

LAG
---

### Ejercicio 15

Mostrar por cada producto de una orden, la cantidad vendida y la cantidad vendida del producto previo..

```sql
select
    od.order_id,
    od.product_id,
    od.quantity,
    lag(od.quantity) over (partition by od.order_id order by od.product_id) as prev_quantity
from
    order_details od;
```

### Ejercicio 16

Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente y última fecha de orden.

```sql
select
    o.order_id,
    o.order_date,
    o.customer_id,
    lag(o.order_date) over (partition by o.customer_id order by o.order_date) as last_order_date
from
    orders o;
```

### Ejercicio 17

Obtener un listado de productos que contengan: id de producto, nombre del producto, precio unitario, precio del producto anterior, diferencia entre el precio del producto y precio del producto anterior.

```sql
select
    table1.product_id,
    table1.product_name,
    table1.unit_price,
    table1.last_unit_price,
    table1.unit_price - table1.last_unit_price as price_difference
from (
    select
        p.product_id,
        p.product_name,
        p.unit_price,
        lag(p.unit_price) over (order by p.product_id) as last_unit_price
    from
        products p
) table1;
```

LEAD
----

### Ejercicio 18

Obtener un listado que muestra el precio de un producto junto con el precio del producto siguiente.

```sql
select
    p.product_name,
    p.unit_price,
    lead(p.unit_price) over (order by p.product_id) as next_price
from
    products p;
```

### Ejercicio 19

Obtener un listado que muestra el total de ventas por categoría de producto junto con el total de ventas de la categoría siguiente.

```sql
select
    table1.category_name,
    table1.total_sales,
    lead(table1.total_sales) over (order by table1.category_name) as next_total_sales
from (
    select
        c.category_name,
        sum(od.unit_price * od.quantity) as total_sales
    from
        order_details od
        left join products p on od.product_id = p.product_id
        left join categories c on p.category_id = c.category_id
    group by
        c.category_name
    order by
        c.category_name
) table1;
```
