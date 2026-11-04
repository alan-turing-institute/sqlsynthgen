.. _page-introduction:

Tutorial: Generate Synthetic Data from PostgreSQL
================================================================

Let us begin with a simple movie rental database called `Pagila <https://github.com/devrimgunduz/pagila>`_. Follow the instructions there to create a PostgreSQL database if you want to follow this tutorial along.
Pagila is already fake data, but we shall pretend that it has sensitive data in it, and we are attempting to keep this data secure.
We will imagine we have a strict protocol to follow to keep the data safe, and that the source database is only accessible from a private network.

You can give access to this database to a different user (I'm using ``tim``) like this:

.. code-block:: console

  $ sudo -u postgres psql pagila
  pagila=# grant pg_read_all_data to tim;
  pagila=# \q

Minimal example
---------------

Let us begin in the private network that this sensitive data resides in (well, let us pretend anyway).

We being by setting the database connection information
(you don't need to set ``SRC_SCHEMA`` if the schema is the default, but for explicitness we do here),
and creating the configuration, ORM and initial statistics files.
For creating a default configuration we will use the ``configure-tables``
command but quit immediately.
Here we are imagining the username is ``postgres`` and the password is ``password`` -- change ``postgres:password`` to the username and password you used to set up the database:

.. code-block::

    $ export SRC_DSN='postgresql://postgres:password@localhost/pagila'
    $ export SRC_SCHEMA='public'
    $ datafaker make-tables
    $ datafaker configure-tables
    Interactive table configuration (ignore, vocabulary, private, generate or empty). Type ? for help.
    (table: actor) quit
    You have made no changes.
    Do you want to save this configuration?
    (yes/no/cancel) yes
    $ datafaker make-stats

This generates the files ``orm.yaml``, ``config.yaml`` and ``src-stats.yaml``.

Now at this point in the process we examine these files for evidence of sensitive information.
At this point there is pretty much nothing in ``config.yaml`` or ``src-stats.yaml``,
and ``orm.yaml`` only contains information on the structure of the source database.
Therefore there is nothing much to examine here, and we can happily take these
three files out of the private network and use them to generate some fake data.

Let us first create a new database within PostgreSQL.
Here we are using user ``tim`` and the default schema ``public``:

.. code-block:: console

    $ sudo -u postgres psql
    postgres=# create database fake_pagila;
    CREATE DATABASE
    postgres=# grant all privileges on database fake_pagila to tim;
    GRANT
    postgres=# \connect fake_pagila
    You are now connected to database "fake_pagila" as user "postgres".
    fake_pagila=# grant all privileges on schema public to tim;
    GRANT
    fake_pagila=# exit

And let's populate it with the fake data:

.. code-block:: shell

    export DST_DSN='postgresql://tim:password@localhost/fake_pagila'
    export DST_SCHEMA='public'
    datafaker create-tables
    datafaker create-data

You will notice that ``create-tables`` produces a couple of warnings, and PostgreSQL complains when ``datafaker`` tries to create the data.
The warnings are that ``datafaker`` doesn't understand the special PostgresSQL types ``TSVECTOR`` and ``ARRAY``, so it doesn't know how to generate data for those columns.
Because it doesn't know how to generate data for those columns it will just use NULLs, and the ``film.fulltext`` column cannot be NULL, so creating the data fails.

Making the minimal example work at all
--------------------------------------

We can use the ``configure-generators`` command to fix this.
Let us add a nice text generator to the ``film.fulltext``
and ``film.special_features`` columns, and while we are at it let's
give the actors sensible names (this would be back in the private
network in a real example).

Here we can see the use of some of the commands available within
the ``configure-generators`` command:

* ``next``: move to the next column or table.
* ``next table-name``: move to table ``table-name``.
* ``propose``: show generators that are available.
* ``compare``: compare the proposed generators' output against the real data.
* ``set``: set the proposed generator as the one we want.
* An empty line repeats the previous command.

.. code-block::

  $ datafaker configure-generators
  Interactive generator configuration. Type ? for help.

  (actor.actor_id[pk]) next
  (actor.first_name) propose
  Sample of actual source data: 'PENELOPE'; 'MINNIE'; 'NICK'; 'FAY'; 'RIP'...
  1. dist_gen.weighted_choice: (fit: 0) 'SIDNEY'; 'KENNETH'; 'EWAN'; 'AL'; 'WHOOPI' ...
  2. dist_gen.weighted_choice [sampled]: (fit: 0) 'MERYL'; 'BEN'; 'DAN'; 'CHARLIZE'; 'SPENCER' ...
  3. dist_gen.choice: (fit: 0.00281) 'JENNIFER'; 'GOLDIE'; 'MILLA'; 'FRANCES'; 'CHRIS' ...
  4. dist_gen.choice [sampled]: (fit: 0.00281) 'RENEE'; 'ANNE'; 'RIP'; 'VAL'; 'SCARLETT' ...
  5. dist_gen.zipf_choice: (fit: 0.0919) 'GROUCHO'; 'THORA'; 'GROUCHO'; 'PENELOPE'; 'KENNETH' ...
  6. dist_gen.zipf_choice [sampled]: (fit: 0.0919) 'KIRK'; 'JOHN'; 'KENNETH'; 'RUSSELL'; 'DARYL' ...
  7. generic.text.word: (fit: 200) 'optimal'; 'suggest'; 'share'; 'principal'; 'contain' ...
  8. generic.person.language: (fit: 284) 'Haitian Creole'; 'Latvian'; 'Somali'; 'Haitian Creole'; 'Belarusian' ...
  9. generic.person.nationality: (fit: 304) 'Afghan'; 'Finnish'; 'Costa Rican'; 'Guatemalan'; 'Afghan' ...
  10. generic.person.last_name: (fit: 313) 'Blake'; 'Weeks'; 'Castillo'; 'Hensley'; 'Soto' ...
  11. generic.person.first_name: (fit: 318) 'Maryln'; 'Minna'; 'Cleo'; 'Efrain'; 'Bart' ...
  12. generic.address.street_name: (fit: 319) 'Lagangreen'; 'Creggan'; 'Killeen'; 'Kilcurragh'; 'Ceylon' ...
  ... lines removed ...
  38. dist_gen.constant: (no fit) ''; ''; ''; ''; '' ...
  (actor.first_name) compare 4 11
  Not private
  4. dist_gen.choice [sampled] requires the following data from the source database:
  SELECT first_name AS value FROM (SELECT first_name FROM actor WHERE first_name IS NOT NULL ORDER BY RANDOM() LIMIT 500) AS _inner GROUP BY value ORDER BY COUNT(first_name) DESC; providing the following values: ['KENNETH', 'PENELOPE', 'JULIA', 'BURT', 'GENE', 'DAN', 'MATTHEW', 'GROUCHO', 'MORGAN', 'RUSSELL', 'CUBA', 'CHRISTIAN', 'ED', 'FAY', 'CAMERON', 'NICK', 'JAYNE', 'SCARLETT', 'AUDREY', 'WOODY', 'ADAM', 'LUCILLE', 'MICHAEL', 'DARYL', 'CHRISTOPHER', 'MARY', 'BEN', 'HUMPHREY', 'MENA', 'CATE', 'RIP', 'REESE', 'MILLA', 'SUSAN', 'KEVIN', 'ANGELA', 'GARY', 'FRANCES', 'SPENCER', 'SEAN', 'KIRSTEN', 'MINNIE', 'CHRIS', 'TOM', 'WARREN', 'RENEE', 'GRETA', 'ALBERT', 'MERYL', 'SANDRA', 'JOHNNY', 'VIVIEN', 'JIM', 'HELEN', 'GINA', 'HARRISON', 'MEG', 'GEOFFREY', 'CHARLIZE', 'JOE', 'CARY', 'FRED', 'MAE', 'RIVER', 'DEBBIE', 'LIZA', 'NATALIE', 'HENRY', 'SIDNEY', 'BETTE', 'OLYMPIA', 'KARL', 'ANNE', 'JANE', 'RALPH', 'LISA', 'ZERO', 'GOLDIE', 'BELA', 'PARKER', 'JENNIFER', 'EMILY', 'JADA', 'WILLIAM', 'HARVEY', 'JODIE', 'ELVIS', 'SALMA', 'GREG', 'SYLVESTER', 'ALEC', 'ELLEN', 'JAMES', 'SISSY', 'IAN', 'KIRK', 'WILL', 'THORA', 'LAURA', 'ANGELINA', 'LAURENCE', 'WHOOPI', 'JUDE', 'TIM', 'KIM', 'OPRAH', 'EWAN', 'VAL', 'CARMEN', 'GREGORY', 'JESSICA', 'JULIANNE', 'RICHARD', 'JEFF', 'AL', 'UMA', 'RAY', 'WALTER', 'MICHELLE', 'JUDY', 'JOHN', 'JON', 'DUSTIN', 'BOB', 'ROCK', 'GRACE', 'RITA', 'ALAN']
  11. generic.person.first_name requires no data from the source database.
  +----------+------------------------------+-------------------------------+
  |  source  | 4. dist_gen.choice [sampled] | 11. generic.person.first_name |
  +----------+------------------------------+-------------------------------+
  |   CATE   |           HARRISON           |             Alden             |
  | KENNETH  |            HENRY             |            Michale            |
  |  MORGAN  |           KIRSTEN            |             Foster            |
  |  SANDRA  |             JUDE             |             Delpha            |
  |  WHOOPI  |             KIRK             |              Kina             |
  |   CUBA   |           PENELOPE           |            Vernetta           |
  |    ED    |           RUSSELL            |           Kristopher          |
  |   JOE    |             JANE             |            Geraldo            |
  |   FAY    |             LIZA             |             Claude            |
  |   JON    |             JUDY             |             Cesar             |
  | SCARLETT |           HARRISON           |            Genevive           |
  |  WARREN  |            CHRIS             |             Hisako            |
  |  WOODY   |            CHRIS             |              Reed             |
  |   MENA   |             JON              |              Mike             |
  |   FAY    |            DARYL             |             Erwin             |
  |  JULIA   |            JAMES             |            Katharyn           |
  |  CHRIS   |            SUSAN             |             Ellis             |
  | HARRISON |             JUDE             |             Orval             |
  |  HARVEY  |            BETTE             |             Kenia             |
  |  SUSAN   |            JULIA             |            Antonia            |
  +----------+------------------------------+-------------------------------+
  (actor.first_name) set 11
  (actor.last_name) propose
  Sample of actual source data: 'CRONYN'; 'DEGENERES'; 'BRODY'; 'DENCH'; 'CAGE'...
  1. dist_gen.weighted_choice: (fit: 0) 'JACKMAN'; 'STREEP'; 'WINSLET'; 'BALL'; 'CAGE' ...
  2. dist_gen.weighted_choice [sampled]: (fit: 0) 'KILMER'; 'LEIGH'; 'WEST'; 'MCKELLEN'; 'HOPKINS' ...
  3. dist_gen.choice: (fit: 0.00396) 'NOLTE'; 'DEGENERES'; 'WINSLET'; 'VOIGHT'; 'LOLLOBRIGIDA' ...
  4. dist_gen.choice [sampled]: (fit: 0.00396) 'AKROYD'; 'BALL'; 'PALTROW'; 'KEITEL'; 'MCQUEEN' ...
  5. dist_gen.zipf_choice: (fit: 0.1) 'GARLAND'; 'HOPKINS'; 'WILLIS'; 'PFEIFFER'; 'PECK' ...
  6. dist_gen.zipf_choice [sampled]: (fit: 0.1) 'BERRY'; 'KILMER'; 'ALLEN'; 'TEMPLE'; 'KILMER' ...
  7. generic.text.word: (fit: 199) 'exploration'; 'substantial'; 'capacity'; 'jam'; 'suicide' ...
  8. generic.address.city: (fit: 233) 'Felixstowe'; 'Bridgend'; 'Peterlee'; 'Kelso'; 'Harwich' ...
  9. generic.address.street_name: (fit: 240) 'Derrynahone'; 'Foster'; 'Esdale'; 'Greenridge'; 'Harris' ...
  10. generic.address.country: (fit: 271) 'Canada'; 'St. Kitts & Nevis'; 'Burkina Faso'; 'Jordan'; 'Lithuania' ...
  11. generic.person.nationality: (fit: 278) 'Latvian'; 'British'; 'Spanish'; 'Russian'; 'Spanish' ...
  12. generic.person.language: (fit: 284) 'Portuguese'; 'Bengali'; 'Dhivehi'; 'Catalan'; 'Portuguese' ...
  13. generic.person.last_name: (fit: 285) 'Hanson'; 'Bush'; 'Benjamin'; 'Cox'; 'Cleveland' ...
  14. generic.person.first_name: (fit: 289) 'Thi'; 'Huey'; 'Gaylord'; 'Marcel'; 'Dong' ...
  15. generic.address.street_suffix: (fit: 331) 'Crescent'; 'Terrace'; 'Mall'; 'Shore'; 'Extension' ...
  ... lines removed ...
  38. dist_gen.constant: (no fit) ''; ''; ''; ''; '' ...
  (actor.last_name) compare 4 13
  Not private
  4. dist_gen.choice [sampled] requires the following data from the source database:
  SELECT last_name AS value FROM (SELECT last_name FROM actor WHERE last_name IS NOT NULL ORDER BY RANDOM() LIMIT 500) AS _inner GROUP BY value ORDER BY COUNT(last_name) DESC; providing the following values: ['KILMER', 'TEMPLE', 'NOLTE', 'WILLIS', 'PECK', 'GUINESS', 'DAVIS', 'DEGENERES', 'HOFFMAN', 'GARLAND', 'BERRY', 'ALLEN', 'TORN', 'KEITEL', 'HARRIS', 'JOHANSSON', 'ZELLWEGER', 'AKROYD', 'HOPKINS', 'WILLIAMS', 'CRONYN', 'DEPP', 'JACKMAN', 'HOPPER', 'DUKAKIS', 'TRACY', 'MONROE', 'MOSTEL', 'MCKELLEN', 'WAHLBERG', 'DEAN', 'BENING', 'SILVERSTONE', 'WEST', 'HACKMAN', 'BOLGER', 'MCQUEEN', 'DENCH', 'DEE', 'NEESON', 'STREEP', 'CAGE', 'BRODY', 'WINSLET', 'WOOD', 'GOODING', 'PENN', 'MCCONAUGHEY', 'CHASE', 'BAILEY', 'PALTROW', 'TANDY', 'CRAWFORD', 'FAWCETT', 'OLIVIER', 'CARREY', 'JOLIE', 'BACALL', 'TOMEI', 'PESCI', 'TAUTOU', 'LEIGH', 'COSTNER', 'WITHERSPOON', 'BASINGER', 'PITT', 'WILSON', 'BRIDGES', 'HUNT', 'GIBSON', 'HESTON', 'SUVARI', 'SINATRA', 'ASTAIRE', 'BULLOCK', 'JOVOVICH', 'GABLE', 'MALDEN', 'CHAPLIN', 'MANSFIELD', 'HURT', 'MCDORMAND', 'BALL', 'PRESLEY', 'RYDER', 'BIRCH', 'BERGMAN', 'WALKEN', 'HOPE', 'BERGEN', 'CRUZ', 'NICHOLSON', 'PHOENIX', 'PFEIFFER', 'SWANK', 'STALLONE', 'BLOOM', 'GRANT', 'SOBIESKI', 'CRUISE', 'BARRYMORE', 'CROWE', 'BALE', 'DAY-LEWIS', 'WAYNE', 'LOLLOBRIGIDA', 'HAWKE', 'MARX', 'POSEY', 'DUNST', 'DAMON', 'PINKETT', 'VOIGHT', 'MIRANDA', 'DERN', 'REYNOLDS', 'GOLDBERG', 'HUDSON', 'DREYFUSS', 'WRAY', 'CLOSE']
  13. generic.person.last_name requires no data from the source database.
  +-----------+------------------------------+------------------------------+
  |   source  | 4. dist_gen.choice [sampled] | 13. generic.person.last_name |
  +-----------+------------------------------+------------------------------+
  |   BRODY   |            TRACY             |           Lambert            |
  |   CARREY  |            CRONYN            |          Cleveland           |
  |  PFEIFFER |           MIRANDA            |           Bullock            |
  |   LEIGH   |            TEMPLE            |           Mckenzie           |
  |   MALDEN  |           WAHLBERG           |            Sawyer            |
  |   HAWKE   |          ZELLWEGER           |            Warner            |
  | JOHANSSON |            HOPPER            |            Ortiz             |
  |    PECK   |           PINKETT            |           Hubbard            |
  |  GOODING  |            CLOSE             |           Burnett            |
  |    CAGE   |          MANSFIELD           |           Erickson           |
  |   BERRY   |            POSEY             |           Russell            |
  |   HOPPER  |            PESCI             |            Combs             |
  |  JACKMAN  |             DEE              |            Hooper            |
  |  PALTROW  |            NOLTE             |           Foreman            |
  |   ALLEN   |             DERN             |            Miles             |
  |  MCKELLEN |            KEITEL            |           Salinas            |
  |   TEMPLE  |          DAY-LEWIS           |           Swanson            |
  |   BERRY   |             TORN             |           Hensley            |
  |   TRACY   |            CRONYN            |             Key              |
  | BARRYMORE |           HACKMAN            |            Cooke             |
  +-----------+------------------------------+------------------------------+
  (actor.last_name) set 13
  (actor.last_update) next film
  (film.description) next
  (film.film_id[pk])
  (film.fulltext) propose
  Sample of actual source data: "'amaz':4 'astronaut':11 'berlin':18 'display':5 'fight':14 'idaho':2 'must':13 'robot':8 'woman':16 'yentl':1"; "'battl':14 'boat':20 'butler':11,16 'command':2 'jet':19 'montezuma':1 'must':13 'reflect':5 'thrill':4 'waitress':8"; "'amaz':4 'astronaut':11 'boy':8 'crusad':2 'epistl':5 'gaslight':1 'gulf':19 'man':16 'mexico':21 'must':13 'redeem':14"; "'arsenic':2 'astronaut':11 'australia':18 'display':5 'girl':8 'lacklustur':4 'must':13 'student':16 'succumb':14 'videotap':1"; "'battl':14 'beauti':4 'brooklyn':1 'compos':11 'dentist':8 'desert':2 'drama':5 'first':20 'man':21 'must':13 'space':22 'station':23 'sumo':16 'wrestler':17"...
  1. dist_gen.choice [sampled]: (fit: 0) "'administr':18 'boat':9,22 'cat':12 'charact':5 'crane':2 'databas':17 'fate':4 'find':15 'jet':21 'must':14 'right':1 'studi':6"; "'baloon':19 'confront':14 'drama':5 'epic':4 'explor':11 'factori':20 'hunter':16 'invas':2 'lumberjack':8 'must':13 'sundanc':1"; "'butler':16 'drama':5 'feminist':11 'hustler':2 'loser':1 'must':13 'nigeria':18 'outgun':14 'robot':8 'stun':4"; "'abandon':21 'beauti':1 'compos':10 'display':7 'fast':5 'fast-pac':4 'greas':2 'mine':22 'moos':13 'must':15 'pace':6 'robot':18 'shaft':23 'sink':16"; "'astronaut':12 'california':19 'car':17 'challeng':15 'cow':9 'epic':4 'hard':2 'juggler':1 'mad':8 'must':14 'stori':5" ...
  2. dist_gen.weighted_choice [sampled]: (fit: 0) "'convent':21 'explor':12 'frisbe':17 'must':14 'mysql':20 'reflect':5 'sink':15 'sleep':1 'stun':4 'sumo':8 'suspect':2 'wrestler':9"; "'administr':18 'boy':8 'challeng':15 'cow':12 'databas':17 'desert':22 'guy':2 'mad':11 'must':14 'sahara':21 'stori':5 'trap':1 'unbeliev':4"; "'amaz':4 'australia':18 'compos':16 'crocodil':8 'crusad':2 'deep':1 'discov':14 'must':13 'squirrel':11 'tale':5"; "'convent':22 'cow':18 'discov':15 'forens':11 'insight':4 'mad':17 'man':8 'must':14 'mysql':21 'psychologist':12 'punk':2 'saga':5 'seabiscuit':1"; "'bore':4 'challeng':14 'dog':11 'fiddler':1 'gulf':19 'lost':2 'madman':16 'mexico':21 'must':13 'squirrel':8 'tale':5" ...
  3. dist_gen.zipf_choice [sampled]: (fit: 0.0346) "'charact':5 'crocodil':12 'gulf':20 'insight':4 'intent':2 'mexico':22 'must':14 'sink':15 'streetcar':1 'studi':6 'waitress':9,17"; "'australia':18 'butler':8 'explor':16 'freddi':2 'must':13 'pursu':14 'saga':5 'sister':1 'stun':4 'woman':11"; "'administr':9 'baloon':21 'chef':13 'coma':2 'confront':16 'databas':8 'emot':4 'factori':22 'metropoli':1 'must':15 'pastri':12 'saga':5 'teacher':18"; "'boat':8 'california':18 'cat':16 'die':1 'intrepid':4 'kill':14 'maker':2 'monkey':11 'must':13 'tale':5"; "'battl':15 'chef':9 'giant':2 'monkey':12,17 'must':14 'pastri':8 'princess':1 'shark':20 'tank':21 'thrill':4 'yarn':5" ...
  (film.fulltext) set 1
  (film.language_id) next
  (film.last_update)
  (film.length)
  (film.original_language_id)
  (film.rating)
  (film.release_year)
  (film.rental_duration)
  (film.rental_rate)
  (film.replacement_cost)
  (film.special_features)
  (film.title) previous
  (film.special_features) propose
  Sample of actual source data: ['Commentaries', 'Deleted Scenes', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes']...
  1. dist_gen.weighted_choice: (fit: 0) ['Trailers', 'Commentaries']; ['Commentaries']; ['Commentaries']; ['Behind the Scenes']; ['Trailers'] ...
  2. dist_gen.weighted_choice [suppressed]: (fit: 0) ['Commentaries', 'Deleted Scenes']; ['Deleted Scenes']; ['Trailers', 'Behind the Scenes']; ['Trailers', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes'] ...
  3. dist_gen.weighted_choice [sampled]: (fit: 0) ['Commentaries', 'Deleted Scenes']; ['Commentaries', 'Deleted Scenes', 'Behind the Scenes']; ['Trailers', 'Deleted Scenes']; ['Trailers', 'Deleted Scenes']; ['Commentaries', 'Deleted Scenes', 'Behind the Scenes'] ...
  4. dist_gen.weighted_choice [sampled and suppressed]: (fit: 0) ['Trailers', 'Commentaries', 'Deleted Scenes']; ['Trailers', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes']; ['Trailers', 'Deleted Scenes']; ['Commentaries', 'Deleted Scenes', 'Behind the Scenes'] ...
  5. dist_gen.choice [sampled]: (fit: 1.26) ['Deleted Scenes', 'Behind the Scenes']; ['Trailers', 'Deleted Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Trailers', 'Commentaries']; ['Deleted Scenes'] ...
  6. dist_gen.choice [sampled and suppressed]: (fit: 1.26) ['Trailers', 'Commentaries']; ['Trailers', 'Commentaries']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Commentaries', 'Deleted Scenes', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes'] ...
  7. dist_gen.choice: (fit: 2.75) ['Deleted Scenes']; ['Trailers', 'Deleted Scenes']; ['Trailers', 'Behind the Scenes']; ['Trailers', 'Deleted Scenes', 'Behind the Scenes']; ['Deleted Scenes'] ...
  8. dist_gen.choice [suppressed]: (fit: 2.75) ['Trailers', 'Behind the Scenes']; ['Deleted Scenes']; ['Trailers', 'Deleted Scenes', 'Behind the Scenes']; ['Trailers', 'Commentaries']; ['Commentaries', 'Deleted Scenes', 'Behind the Scenes'] ...
  9. dist_gen.zipf_choice [sampled]: (fit: 71.1) ['Commentaries', 'Deleted Scenes']; ['Commentaries', 'Deleted Scenes']; ['Trailers', 'Deleted Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Deleted Scenes'] ...
  10. dist_gen.zipf_choice [sampled and suppressed]: (fit: 71.1) ['Commentaries', 'Deleted Scenes']; ['Trailers', 'Commentaries', 'Deleted Scenes', 'Behind the Scenes']; ['Trailers']; ['Trailers']; ['Commentaries', 'Deleted Scenes'] ...
  11. dist_gen.zipf_choice: (fit: 299) ['Trailers']; ['Trailers', 'Commentaries']; ['Trailers', 'Commentaries']; ['Trailers']; ['Commentaries'] ...
  12. dist_gen.zipf_choice [suppressed]: (fit: 299) ['Trailers']; ['Trailers', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Trailers', 'Commentaries', 'Behind the Scenes']; ['Trailers'] ...
  13. dist_gen.constant: (no fit) None; None; None; None; None ...
  (film.special_features) set 1
  (film.title) quit
  Table film:
  ...changing special_features from nothing to dist_gen.weighted_choice
  Do you want to save this configuration?
  (yes/no/cancel) yes

Let's have a look at what that did to ``config.yaml``
(which used to contain almost nothing):

.. code-block:: yaml

  src-stats:
  - comments:
    - The values that appear in column fulltext of a random sample of 500 rows of table
      film
    name: auto__film__fulltext
    query: SELECT fulltext AS value FROM (SELECT fulltext FROM film WHERE fulltext IS
      NOT NULL ORDER BY RANDOM() LIMIT 500) AS _inner GROUP BY value ORDER BY COUNT(fulltext)
      DESC
  tables:
    actor:
      row_generators:
      - columns_assigned:
        - first_name
        name: generic.person.first_name
      - columns_assigned:
        - last_name
        name: generic.person.last_name
    address: {}
    category: {}
    city: {}
    country: {}
    customer: {}
    film:
      row_generators:
      - columns_assigned:
        - fulltext
        kwargs:
          a: SRC_STATS["auto__film__fulltext"]["results"]
        name: dist_gen.choice
      - columns_assigned:
        - special_features
        kwargs:
          a: SRC_STATS["auto__film__special_features"]["results"]
        name: dist_gen.weighted_choice
    film_actor: {}
    film_category: {}
    inventory: {}
    language: {}
    payment: {}
    payment_p2022_01: {}
    payment_p2022_02: {}
    payment_p2022_03: {}
    payment_p2022_04: {}
    payment_p2022_05: {}
    payment_p2022_06: {}
    payment_p2022_07: {}
    rental: {}
    staff: {}
    store: {}

Here we can see the simple generators that are now applied to the first name
and last name of the ``actor`` table. We can also see the ``dist_gen.choice``
generators we chose for ``film.fulltext`` and ``film.special_features``, and how
these require some data from the database.
We can see the query that will provide this data in the ``src-stats:`` block.

However, this was a confusing choice to make. We did not see actual text when we
were choosing the generator, we saw a list of words and numbers.

Well, let's see what this generates anyway. We will need to use the
``--force`` option for overwrite the existing files, and the ``--num-passes``
option to create multiple rows of output.

.. code-block:: shell

  datafaker create-data --num-passes 3

Now let's have a look at what data we have in the destination database:

.. code-block::

  $ datafaker list-tables
  actor
  address
  category
  city
  country
  customer
  film
  film_actor
  film_category
  inventory
  language
  payment
  payment_p2022_01
  payment_p2022_02
  payment_p2022_03
  payment_p2022_04
  payment_p2022_05
  payment_p2022_06
  payment_p2022_07
  rental
  staff
  store
  $ datafaker dump-data --output - --table actor
  actor_id,first_name,last_name,last_update
  1,Vertie,Huber,2026-12-13 01:09:43.409289+00:00
  2,Jewel,Clarke,2026-10-31 16:07:09.691557+00:00
  3,Arden,Chavez,2026-03-12 17:22:18.332749+00:00
  $ datafaker dump-data --output - --table film
  description,film_id,fulltext,language_id,last_update,length,original_language_id,rating,release_year,rental_duration,rental_rate,replacement_cost,special_features,title
  Green,1,'amaz':4 'boat':22 'butler':8 'drama':5 'expec':1 'husband':11 'must':13 'natur':2 'reach':14 'shark':17 'u':21 'u-boat':20,1,2026-04-22 19:56:57.650072+01:00,938,1,Cyan,-319,-52,79.21,45.44,"['Deleted Scenes', 'Behind the Scenes']",Blue
  Red,2,'ballroom':2 'boondock':1 'boy':11 'crocodil':8 'defeat':14 'fate':4 'gulf':19 'mexico':21 'monkey':16 'must':13 'panorama':5,1,2026-08-08 21:06:59.620631+01:00,994,1,Brown,-471,-388,97.80,32.94,"['Commentaries', 'Deleted Scenes', 'Behind the Scenes']",Blue
  Magenta,3,"'ancient':20 'astound':4 'chanc':1 'china':21 'forens':8,12 'moos':18 'must':15 'overcom':16 'psychologist':9,13 'resurrect':2 'stori':5",2,2026-06-10 09:43:23.457110+01:00,273,2,Pink,520,-960,71.36,14.38,"['Trailers', 'Behind the Scenes']",Brown
  $ datafaker dump-data --output - --table film_actor
  actor_id,film_id,last_update
  1,1,2026-07-18 22:09:15.669313+01:00
  2,2,2026-02-18 03:04:45.317350+00:00
  3,3,2026-07-09 20:43:06.250462+01:00

So here we have dumped the two tables we configured (``actor`` and ``film``),
and one other (``film_actor``), three rows of each because we specified ``--num-passes 3``.

You will see that almost all of the columns have correctly-typed data in it.
The primary keys befin at one and increase by one per row,
all the foreign keys point to existing rows in the correct table
and all the data is correctly-typed. Also our nice new generators are working:
Our ``actor`` table has nice names in it, and our ``film`` table has a ``fulltext`` column.

Problems with the minimal example
---------------------------------

Here is a non-exhaustive list of issues with the data produced:

- all text fields are just colours, for example:
  - staff names (we can deal with this the same way we dealt with actors names above).
  - address lines.
  - movie categories.
  - city, country and language names.
  - movie descriptions.
- the ``fulltext`` table in Pagila relates to the ``title`` and ``description``
  but in our generated data does not.
- there are a lot of payment tables that are partitions of the
  main payment table in the source database, but these are
  just different tables in the generated table.

Fixing the problems with the minimal example #1: ignoring unwanted tables
-------------------------------------------------------------------------

First, let us remove all the ``payment_`` tables.
This lowers the fidelity of the generated database, but ``datafaker`` cannot cope with partitioned tables
so the best that we can do is pretend that ``payment`` is not a partitioned table.
If we think that our users will not be interested in this implementation detail then this will be acceptable.

We fix this problems with ``datafaker configure-tables``
(remember that entering no command repeats the previous command):

.. code-block::

  $ datafaker configure-tables
  Interactive table configuration (ignore, vocabulary, private, generate or empty). Type ? for help.

  (table: actor) next
  (table: address)
  (table: category)
  (table: city)
  (table: country)
  (table: customer)
  (table: film)
  (table: film_actor)
  (table: film_category)
  (table: inventory)
  (table: language)
  (table: payment)
  (table: payment_p2022_01) ignore
  Table payment_p2022_01 set as ignored
  (table: payment_p2022_02)
  Table payment_p2022_02 set as ignored
  (table: payment_p2022_03)
  Table payment_p2022_03 set as ignored
  (table: payment_p2022_04)
  Table payment_p2022_04 set as ignored
  (table: payment_p2022_05)
  Table payment_p2022_05 set as ignored
  (table: payment_p2022_06)
  Table payment_p2022_06 set as ignored
  (table: payment_p2022_07)
  Table payment_p2022_07 set as ignored
  (table: rental) quit
  Changing payment_p2022_01 from generate to ignore
  Changing payment_p2022_02 from generate to ignore
  Changing payment_p2022_03 from generate to ignore
  Changing payment_p2022_04 from generate to ignore
  Changing payment_p2022_05 from generate to ignore
  Changing payment_p2022_06 from generate to ignore
  Changing payment_p2022_07 from generate to ignore
  Do you want to save this configuration?
  (yes/no/cancel) yes


This has changed ``config.yaml`` file by adding the following sections:

.. code-block:: yaml

    payment_p2022_01:
      ignore: true
    payment_p2022_02:
      ignore: true
    payment_p2022_03:
      ignore: true
    payment_p2022_04:
      ignore: true
    payment_p2022_05:
      ignore: true
    payment_p2022_06:
      ignore: true
    payment_p2022_07:
      ignore: true

Now we can destroy the existing database and try again:

.. code-block:: shell

  datafaker remove-tables --all --yes
  datafaker create-tables
  datafaker create-data --num-passes 3

We don't need to regenerate the generators this time as we have not changed anything in the ``config.yaml`` file that affects generators.

Fixing the problems with the minimal example #2: generate vocabularies
----------------------------------------------------------------------

While we could try to generate random plausible language, country, city and film category names, there is a better way.
As these tables hold no sensitive data, we can just copy them.

We will need to use ``datafaker configure-tables`` again; in fact,
we could have done this at the same time as ignoring the payment partitions.
After configuring the tables we use ``datafaker make-vocab`` to export the data:

.. code-block::

  $ datafaker configure-tables
  Interactive table configuration (ignore, vocabulary, private, generate or empty). Type ? for help.

  (table: actor) next
  (table: address)
  (table: category) vocabulary
  Table category set as vocabulary
  (table: city)
  Table city set as vocabulary
  (table: country)
  Table country set as vocabulary
  (table: customer) next
  (table: film)
  (table: film_actor)
  (table: film_category)
  (table: inventory)
  (table: language) vocabulary
  Table language set as vocabulary
  (table: payment) quit
  Changing category from generate to vocabulary
  Changing city from generate to vocabulary
  Changing country from generate to vocabulary
  Changing language from generate to vocabulary
  Do you want to save this configuration?
  (yes/no/cancel) yes
  $ datafaker make-vocab --compress

Now we can see the follwing sections in ``config.yaml`` containing ``vocabulary_table:true``:

.. code-block:: yaml

  category:
    vocabulary_table: true
  city:
    vocabulary_table: true
  country:
    vocabulary_table: true
  language:
    vocabulary_table: true

And we can also see we have generated four files:
``category.yaml.gz``, ``city.yaml.gz``, ``country.yaml.gz`` and ``language.yaml.gz``.
If the ``--compress`` option is not passed it will produce ``.yaml`` files instead of ``.yaml.gz`` and this would be fine in this case.
Certain databases have very large vocabulary tables, for example the ``concept`` table in OMOP databases.
Such huge YAML files can cause problems, but they compress very well, so the ``--compress`` option can be very useful for overcoming such limitations.
Generating these huge vocabulary files can nevertheless take a very long time! Not in Pagila's case, though.

Now your data privacy protocols will either require you to unzip and examine these files before taking them out of the private network
or it will trust ``datafaker`` to produce only non-private output given certain inputs.
In either case we take these files out of the private network.

Using the same ``config.yaml`` file outside the private network (and with ``DST_DSN`` set as above) we delete the existing data in these vocabulary tables,
and fill them with the new data from the ``yaml.gz`` (or unzipped ``.yaml``) files:

.. code-block:: console

  $ datafaker remove-vocab --yes
  tim@tim-Latitude-5410:~/Documents/test$ datafaker create-vocab
  tim@tim-Latitude-5410:~/Documents/test$ datafaker dump-data --output - --table language
  language_id,last_update,name
  1,2022-02-15 10:02:19+00:00,English
  2,2022-02-15 10:02:19+00:00,Italian
  3,2022-02-15 10:02:19+00:00,Japanese
  4,2022-02-15 10:02:19+00:00,Mandarin
  5,2022-02-15 10:02:19+00:00,French
  6,2022-02-15 10:02:19+00:00,German

Fixing the problems with the minimal example #3: generate more plausible text
-----------------------------------------------------------------------------

Let us take the example of the ``film.description`` column. Remember we did this above:

.. code-block::

  $ datafaker dump-data --output - --table film
  description,film_id,fulltext,language_id,last_update,length,original_language_id,rating,release_year,rental_duration,rental_rate,replacement_cost,special_features,title
  Green,1,'amaz':4 'boat':22 'butler':8 'drama':5 'expec':1 'husband':11 'must':13 'natur':2 'reach':14 'shark':17 'u':21 'u-boat':20,1,2026-04-22 19:56:57.650072+01:00,938,1,Cyan,-319,-52,79.21,45.44,"['Deleted Scenes', 'Behind the Scenes']",Blue
  Red,2,'ballroom':2 'boondock':1 'boy':11 'crocodil':8 'defeat':14 'fate':4 'gulf':19 'mexico':21 'monkey':16 'must':13 'panorama':5,1,2026-08-08 21:06:59.620631+01:00,994,1,Brown,-471,-388,97.80,32.94,"['Commentaries', 'Deleted Scenes', 'Behind the Scenes']",Blue
  Magenta,3,"'ancient':20 'astound':4 'chanc':1 'china':21 'forens':8,12 'moos':18 'must':15 'overcom':16 'psychologist':9,13 'resurrect':2 'stori':5",2,2026-06-10 09:43:23.457110+01:00,273,2,Pink,520,-960,71.36,14.38,"['Trailers', 'Behind the Scenes']",Brown

The ``description`` column has values ``Green``, ``Red`` and ``Magenta``. Can we do better? Well, maybe a bit.

Let us at least make the description longer than one word!
``datafaker configure-generators`` using commands ``next table-name.column-name``, ``propose`` and ``set``,
followed by re-generating the data:

.. code-block::

  $ datafaker configure-generators
  Interactive generator configuration. Type ? for help.

  (actor.first_name (generic.person.first_name)) next film.description
  (film.description) propose
  Sample of actual source data: 'A Brilliant Panorama of a Boat And a Astronaut who must Challenge a Teacher in A Manhattan Penthouse'; 'A Lacklusture Epistle of a Boat And a Technical Writer who must Fight a A Shark in The Canadian Rockies'; 'A Awe-Inspiring Drama of a Dog And a Man who must Escape a Robot in A Shark Tank'; 'A Epic Yarn of a Cat And a Madman who must Vanquish a Dentist in An Abandoned Amusement Park'; 'A Intrepid Story of a Student And a Dog who must Challenge a Explorer in Soviet Georgia'...
  1. dist_gen.choice [sampled]: (fit: 0) 'A Emotional Tale of a Robot And a Sumo Wrestler who must Redeem a Pastry Chef in A Baloon Factory'; 'A Insightful Epistle of a Pastry Chef And a Womanizer who must Build a Boat in New Orleans'; 'A Stunning Display of a Moose And a Database Administrator who must Pursue a Composer in A Jet Boat'; 'A Amazing Documentary of a Car And a Robot who must Escape a Lumberjack in An Abandoned Amusement Park'; 'A Beautiful Story of a Monkey And a Sumo Wrestler who must Conquer a A Shark in A MySQL Convention' ...
  2. dist_gen.weighted_choice [sampled]: (fit: 0) 'A Fateful Story of a A Shark And a Explorer who must Succumb a Technical Writer in A Jet Boat'; 'A Epic Tale of a Robot And a Monkey who must Vanquish a Man in New Orleans'; 'A Fast-Paced Panorama of a Technical Writer And a Mad Scientist who must Find a Feminist in An Abandoned Mine Shaft'; 'A Insightful Panorama of a Crocodile And a Boat who must Conquer a Sumo Wrestler in A MySQL Convention'; 'A Brilliant Tale of a Car And a Moose who must Battle a Dentist in Nigeria' ...
  3. dist_gen.zipf_choice [sampled]: (fit: 0.0346) 'A Intrepid Yarn of a Frisbee And a Dog who must Build a Astronaut in A Baloon Factory'; 'A Awe-Inspiring Character Study of a Boy And a Feminist who must Sink a Crocodile in Ancient China'; 'A Thrilling Yarn of a Feminist And a Madman who must Battle a Hunter in Berlin'; 'A Lacklusture Reflection of a Boat And a Forensic Psychologist who must Fight a Waitress in A Monastery'; 'A Insightful Drama of a Mad Scientist And a Hunter who must Defeat a Pastry Chef in New Orleans' ...
  4. generic.text.sentence: (fit: 789) 'Do you come here often?'; 'Haskell features a type system with type inference and lazy evaluation.'; 'Do you have any idea why this is not working?'; 'Erlang is a general-purpose, concurrent, functional programming language.'; 'Ports are used to communicate with the external world.' ...
  5. generic.text.quote: (fit: 826) "Mama always said life was like a box of chocolates. You never know what you're gonna get."; "Mama always said life was like a box of chocolates. You never know what you're gonna get."; 'A census taker once tried to test me. I ate his liver with some fava beans and a nice Chianti.'; 'Houston, we have a problem.'; 'Elementary, my dear Watson.' ...
  6. generic.text.text: (fit: 1600) 'It is also a garbage-collected runtime system. Any element of a tuple can be accessed in constant time. They are written as strings of consecutive alphanumeric characters, the first character being lowercase. The arguments can be primitive data types or compound data types. In 1989 the building was heavily damaged by fire, but it has since been restored.'; 'Atoms can contain any character if they are enclosed within single quotes and an escape convention exists which allows any character to be used within an atom. Where are my pants? Do you have any idea why this is not working? Haskell features a type system with type inference and lazy evaluation. Erlang is a general-purpose, concurrent, functional programming language.'; 'Initially composing light-hearted and irreverent works, he also wrote serious, sombre and religious pieces beginning in the 1930s. Make me a sandwich. Make me a sandwich. The Galactic Empire is nearing completion of the Death Star, a space station with the power to destroy entire planets. Make me a sandwich.'; 'Make me a sandwich. Its main implementation is the Glasgow Haskell Compiler. Atoms can contain any character if they are enclosed within single quotes and an escape convention exists which allows any character to be used within an atom. The Galactic Empire is nearing completion of the Death Star, a space station with the power to destroy entire planets. Haskell features a type system with type inference and lazy evaluation.'; 'The syntax {D1,D2,...,Dn} denotes a tuple whose arguments are D1, D2, ... Dn. They are written as strings of consecutive alphanumeric characters, the first character being lowercase. The sequential subset of Erlang supports eager evaluation, single assignment, and dynamic typing. Erlang is a general-purpose, concurrent, functional programming language. Ports are created with the built-in function open_port.' ...
  ... lines removed ...
  35. dist_gen.constant: (no fit) None; None; None; None; None ...
  (film.description) set 5
  (film.film_id[pk]) quit
  Table film:
  ...changing description from nothing to generic.text.quote
  Do you want to save this configuration?
  (yes/no/cancel) yes
  $ datafaker remove-data --yes
  $ datafaker create-data --num-passes 3
  $ datafaker dump-data --output - --table film
  description,film_id,fulltext,language_id,last_update,length,original_language_id,rating,release_year,rental_duration,rental_rate,replacement_cost,special_features,title
  I'm gonna make him an offer he can't refuse.,1,'apollo':2 'beauti':4 'conquer':15 'convent':22 'monkey':8 'must':14 'mysql':21 'shark':18 'stori':5 'sumo':11 'wild':1 'wrestler':12,2,2026-04-07 16:17:54.353109+01:00,981,2,Brown,-846,575,51.36,73.72,"['Trailers', 'Deleted Scenes']",Cyan
  Those who refuse to learn from history are condemned to repeat it.,2,'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14,3,2026-11-03 01:24:15.123173+00:00,122,5,Magenta,-154,148,46.16,17.17,"['Trailers', 'Deleted Scenes']",Red
  "One morning I shot an elephant in my pajamas. How he got in my pajamas, I don't know.",3,'berlin':20 'car':10 'cat':13 'fargo':2 'fast':5 'fast-pac':4 'hunter':18 'must':15 'outgun':16 'pace':6 'perdit':1 'stori':7,2,2026-12-28 08:16:07.956949+00:00,393,5,Black,761,158,34.31,61.42,"['Deleted Scenes', 'Behind the Scenes']",Pink

So, not really movie descriptions, but at least text of sensible length.
This generator requires no information from the source database and so cannot leak private data.
Later, datafaker might gain the capability to generate more exciting text.

Fixing the problems with the minimal example #4: Unnormalized databases
-----------------------------------------------------------------------

If you look at the ``film`` table in the source Pagila directory, you might figure out that
the ``fulltext`` column is where to find all the words in the ``title`` and ``description`` columns:

.. code-block::

  $ psql pagila
  psql (17.7 (Ubuntu 17.7-3.pgdg24.04+1), server 16.11 (Ubuntu 16.11-1.pgdg24.04+1))
  Type "help" for help.

  pagila=> select title,description,fulltext,to_tsvector(concat(title,' ',description)) from film limit 3;
        title       |                                             description                                              |                                                                  fulltext                                                                   |                                                                 to_tsvector
  ------------------+------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------
  ACADEMY DINOSAUR | A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies     | 'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17 | 'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17
  ACE GOLDFINGER   | A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China | 'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14      | 'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14
  ADAPTATION HOLES | A Astounding Reflection of a Lumberjack And a Car who must Sink a Lumberjack in A Baloon Factory     | 'adapt':1 'astound':4 'baloon':19 'car':11 'factori':20 'hole':2 'lumberjack':8,16 'must':13 'reflect':5 'sink':14                          | 'adapt':1 'astound':4 'baloon':19 'car':11 'factori':20 'hole':2 'lumberjack':8,16 'must':13 'reflect':5 'sink':14
  (3 rows)

So ideally we should be able to generate ``title`` and ``description``, then set the ``fulltext`` column
with the SQL expression ``TO_TSVECTOR(CONCAT(title, ' ', description))``.

Sorry, this is not possible at the moment using the configuration commands;
stories should be used if this column is necessary.
One possible solution is to remove this column from the ``orm.yaml`` file.
Find the portion that corresponds to the ``film`` table in this file:

.. code-block::yaml
  film:
    columns:
      description:
        nullable: true
        primary: false
        type: TEXT
      film_id:
        nullable: false
        primary: true
        type: INTEGER
      fulltext:
        nullable: false
        primary: false
        type: TSVECTOR
      language_id:
        foreign_keys:
        - language.language_id
        nullable: false
        primary: false
        type: INTEGER
      ### ... lines removed ... ###
      title:
        nullable: false
        primary: false
        type: TEXT
    unique: []

Simply remove the ``fulltext:`` section and this column will not appear in the destination database.
