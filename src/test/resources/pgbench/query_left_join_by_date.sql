SET SCHEMA 'test_insertion';

-- \set id random(1027634, 1037634)

select paymentdoc0_.id               as id1_4_,
       paymentdoc0_.account_id       as account10_4_,
       paymentdoc0_.amount           as amount2_4_,
       paymentdoc0_.cur              as cur11_4_,
       paymentdoc0_.expense          as expense3_4_,
       paymentdoc0_.order_date       as order_da4_4_,
       paymentdoc0_.order_number     as order_nu5_4_,
       paymentdoc0_.payment_purpose  as payment_6_4_,
       paymentdoc0_.prop_10          as prop_7_4_,
       paymentdoc0_.prop_15          as prop_8_4_,
       paymentdoc0_.prop_20          as prop_9_4_,
       paymentdoc0_1_.transaction_id as transact2_1_
from payment_document paymentdoc0_
         left outer join active_transaction paymentdoc0_1_ on paymentdoc0_.id = paymentdoc0_1_.id
where (paymentdoc0_1_.transaction_id is null)
  AND paymentdoc0_.order_date = '2025-01-19';
