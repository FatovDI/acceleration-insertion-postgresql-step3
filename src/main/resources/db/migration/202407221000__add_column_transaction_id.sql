CREATE SEQUENCE IF NOT EXISTS seq_trans_id as smallint INCREMENT BY 1 NO MAXVALUE CYCLE;

ALTER TABLE PAYMENT_DOCUMENT ADD COLUMN transaction_id smallint DEFAULT null;
CREATE INDEX IX_PAYMENT_DOCUMENT_transaction_id on PAYMENT_DOCUMENT (transaction_id) where payment_document.transaction_id is not null ;