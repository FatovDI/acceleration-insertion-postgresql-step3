-- ALTER TABLE PAYMENT_DOCUMENT ADD COLUMN transaction_id uuid DEFAULT null;
-- CREATE INDEX IX_PAYMENT_DOCUMENT_transaction_id on PAYMENT_DOCUMENT (transaction_id) where payment_document.transaction_id is not null ;
select 1;