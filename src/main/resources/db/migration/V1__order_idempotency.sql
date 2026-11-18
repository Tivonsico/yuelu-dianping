-- Apply once to the isolated test DB (or after auditing existing duplicate data).
-- This deliberately fails if duplicate user/voucher rows already exist; never delete real orders silently.
ALTER TABLE tb_voucher_order ADD CONSTRAINT uk_voucher_order_user_voucher UNIQUE (user_id, voucher_id);
