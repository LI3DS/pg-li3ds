extdir=`pg_config --sharedir`/extension

install:
	install -m 0644 extension/li3ds.control $(extdir)
	install -m 0644 extension/li3ds--1.0.0.sql $(extdir)

