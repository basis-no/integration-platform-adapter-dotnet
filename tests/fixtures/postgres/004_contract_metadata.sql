-- Store schema version metadata for the Message Store schema (store schema major 1).
-- Change it only as a deliberate schema change in both the .NET and Camel Quarkus adapters.
--
-- Applied by the database operator together with 002 and the runtime role's grants. Adapters read this at
-- startup and refuse to start against an unknown contract_major.

create table if not exists bip_contract (
    contract_major integer not null,
    contract_version text not null,
    applied_at timestamptz not null default current_timestamp
);

insert into bip_contract (contract_major, contract_version)
select 1, '1.0.0-rc2'
where not exists (select 1 from bip_contract where contract_major = 1);
