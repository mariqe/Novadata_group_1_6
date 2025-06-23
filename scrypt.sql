CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

create or replace function audit_user_changes () returns trigger as $$
declare
begin
 	if new.name is distinct from old.name then
 		insert into users_audit(user_id, field_changed, old_value, new_value, changed_by)
 		values (old.id, 'name', old.name, new.name, current_user);
 	end if;
    
    if new.email is distinct from old.email then
    	insert into users_audit(users_id, field_changed, old_value, new_value, changed_by)
    	values (old.id, 'email', old.email, new.email, current_user);
    end if;
    
    if new.role is distinct from old.role then
    	insert into users_audit(user_id, field_changed, old_value,new_value, changed_by)
    	values (old.id, 'role', old.role, new.role, current_user);
    end if;
    
    return new;
 end;
 $$ language plpgsql;

create trigger trigger_audit_user_changes
before update on users
for each row
execute function audit_user_chages();

create extension if not exists pg_cron;

insert into users (name, email, role)
values ('Alice', 'alice@example.com', 'user');

select * from users; 
select * from users_audit ua;

update users set name = 'Alice Smith', email = 'alice.smith@example.com' where id = 1;

create or replace function export_audit_to_csv() returns void as $outer$
declare
	path text := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
begin
	execute format(
		$inner$
		copy (
			select user_id, field_changed, old_value, new_value, changed_by, changed_at
			from users_audit
			where changed_at >= now() - interval '1 day'
			order by changed_at
		) to '%s' with csv header
		$inner$, path
	);
end;
$outer$ language plpgsql;

select cron.schedule(
	job_name := 'daily_audit_export',
	schedule := '0 3 * * *',
	command := $$select export_audit_to_csv();$$
);

select export_audit_to_csv ();


