create table person (name varchar(50) primary key, score int not null, date_of_birth date, registered timestamp);
insert into person(name,score) values('FRED',21);
insert into person(name,score) values('JOSEPH',34);
insert into person(name,score) values('MARMADUKE',25);

create table address (address_id int primary key, full_address varchar(255) not null);
insert into address(address_id, full_address) values(1,'57 Something St, El Barrio, Big Place');
insert into address(address_id, full_address) values(2,'103 Bumblebee Ave, Jumpdown, Townie');

--TODO create table person_address

-- test blobs, clobs
--TODO add primary keys
create table person_document (name varchar(50) not null,  document clob);
create table person_image (name varchar(50) not null, document blob);

-- test auto pk
create table note(id bigint auto_increment primary key, text varchar(255);

