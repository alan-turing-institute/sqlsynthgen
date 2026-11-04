==================
Datafaker Overview
==================

Datafaker provides a way to create a fake version of sensitive data.

Datafaker's workflow permits Information Governance oversight by design.

Background
==========

Conceptually, any faking of sensitive data achieves privacy through one or both of two mechanisms:
Reducing the real data down to summaries, and obfuscating the data by adding noise.

After these privacy-preserving steps, reproducing a fake version of the full steps
involves adding noise to the summary data to turn it back into data with the
required structure and volume. We can call these stages Reduce, Obfuscate,
Repopulate.

.. mermaid::
    :alt: Generic synthetic data flow

    block-beta
    columns 1
    Source["Sensitive Data"]
    block:Arrow1
        ArrowNote1(["Remove Personally\nIdentifiable Data\n/ summarize / other removal"])
        Summary<["Reduce"]>(down)
    end
    block:Intermediate1
        ReducedNote(["This data is less sensitive"])
        Reduced["Reduced data / summaries"]
    end
    block:Arrow2
        ArrowNote2(["Adding noise"])
        Obfuscation<["Obfuscate"]>(down)
    end
    block:Intermediate2
        ObfuscatedNote(["This data is even\nless sensitive"])
        Obfuscated("Obfuscated\nReduced data")
    end
    block:Arrow3
        ArrowNote3(["Replacing removed data\nbased on summaries"])
        Repopulation<["Repopulate"]>(down)
    end
    Destination["Synthetic Data"]
    classDef nobox fill:#fff,stroke-width:0px;
    class Intermediate1 nobox
    class Intermediate2 nobox
    class Arrow1 nobox
    class Arrow2 nobox
    class Arrow3 nobox
    classDef note fill:#18c;
    class ArrowNote1 note
    class ArrowNote2 note
    class ArrowNote3 note
    class ReducedNote note
    class ObfuscatedNote note

In this diagram, Reduce and Obfuscate take place in a private environment such as
a Trusted Research Environment or some machine on a private network from which
the sensitive data can be read. Information Governance oversight happens at the end
of this process to decide if this obfucated summary can be released out of the private
network.

The Repopulate process can happen outside of the private network and outside of the remit
of Information Governance. This is important! The act of turning reduced
obfucated data into a larger quantity of randomized data is *not* a privacy-sheilding
process! By this time all the actual privacy-sheilding has already happened and
the Information Governance restrictions have already been applied.

Reduction techniques
--------------------

Here are a few techniques that can be used to preserve privacy through data reduction.
Most Reduction techniques have a corresponding Repopulation technique
to add the missing data back into the synthetic data.

.. list-table:: Reduction Techniques
   :width: 100%
   :widths: 10 30 20
   :header-rows: 1

   * - Reduction technique
     - Description
     - Repopulation technique
   * - Anonymization
     - Removing Personally Identifiable Information (remembering what sort of information was removed and its format)
     - Generation of Names/Addresses etcetera
   * - Removal of unnecessary data
     - Some data does not need to be supplied and so can be removed completely.
     - None.
   * - Summarization
     - Extract the means, standard deviations and size of numeric data, the frequencies of categorical data, or other summary measures.
     - Pick the new data from a suitable random distribution.
   * - Low number suppression
     - Removal of rare data, which could be used to infer the presence of certain individuals in the data set.
     - None.
   * - Grouping
     - Gathering data into clusters and extracting summary data from each cluster.
     - Picking new data by picking a cluster then picking from the appropriate random distribution.

Each of these extracts less than the full information from the source, then
most of them replace the information left behind with generated information
(perhaps after an Obfuscation step).

Differential Privacy
--------------------

One actual concrete method of faking data is to apply a process such as Differential
Privacy to the real data without summarizing. In such a case Information Governance
cannot be expected to examine the data for privacy breaches as there is the same
amount of data to examine as in the real database, and breaches are likely to be
more subtle than real names being produced; in this case Information Governance
would have to understand and trust the obfucation process.

In the US, The National Institute for Science and Technology has some
`guidelines <https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-226.pdf>`_
about the use of Differential Privacy but even that is at a loss to explain how to
understand its main parameter:

    Selecting privacy loss parameters, such as ε, is challenging,
    and we offer no specific guidelines on their selection.

So Differential Privacy can be useful, but it is very hard to quantify exactly
how much good it is doing to preserve privacy.

Differential Privacy is an Obfuscation technique rather than a Reduction technique
because the same amount of data is output as was input.

Datafaker's Operation
=====================

Datafaker is based on the Alan Turing Institute's
`SqlSynthGen tool <https://github.com/alan-turing-institute/sqlsynthgen>`_.
SqlSynthGen implements all three operations of Reduce, Obfuscate and Repopulate.

Datafaker builds on SqlSynthGen by automating the specification of the Reduce
operation and making the dataflow clearer for Information Governance purposes
(amongst other improvements). Datafaker still retains SqlSynthGen's Obfuscation
by Differential Privacy functionality, but this has not yet been folded into the
automation and will not be described further here: we are relying on the
Reduce operation to provide privacy shielding.

Datafaker has a number of phases of operation; below we describe how
these map to the Reduce, Obfuscate and Repopulate stages described above.

.. mermaid::
    :alt: Simplified diagram of Datafaker's data flow

    block-beta
    columns 1
    Source["Sensitive Data"]
    block:Processing
        columns 4
        Arrow1<["Extract\nstructure"]>(down)
        Arrow2<["User guided\nconfiguration"]>(down)
        Arrow3<["Extract\nsummary"]>(down)
        Arrow4<["Extract\nvocabulary"]>(down)
        Orm["orm.yaml\nstructure\ndefinition"]
        Config["config.yaml\nconfiguration\nof summarization"]
        SrcStats["src-stats.yaml\nSummary\nstatistics"]
        Vocab["Vocabulary\ntable data"]
    end
    Arrow5<["Information Governance\npermits release"]>(down)
    block:Release
        columns 1
        Arrow6<["Repopulation"]>(down)
        Destination["Synthetic Data"]
    end
    classDef nobox fill:#fff,stroke-width:0px;
    class Processing nobox
    classDef publicnet fill:#18c;
    class Release publicnet

Datafaker make-tables phase
---------------------------

``datafaker make-tables`` makes a file called ``orm.yaml`` that describes the structure of the source database.
This is part of the Reduce phase, but this file is used in every other Datafaker phase.
By describing the structure of the database, no private data is leaked.
However, if part of a commercial database's schema is considered commercially sensitive,
this information would be leaked in the ``orm.yaml`` file.
It may be possible to alter this file by hand to remove the sensitive information as long as the YAML structure is maintained.

Datafaker configuration phase
-----------------------------

The following commands are not really part of the Reduce phase, but allow the user to configure
what the Reduce phase will entail (and hence also what the Repopulate phase will entail).

- ``datafaker configure-tables`` makes a file called ``config.yaml`` that describes what needs to happen to each table.
- ``datafaker configure-generators`` amends ``config.yaml`` with information on what happens to each column.
- ``datafaker configure-missingness`` optionally amends ``config.yaml`` with simple summary missingness information,

Additional configuration can be applied by hand at this point to allow more sophisticated Repopulation to happen than the automated generation can manage.

``config.yaml`` contains data that has been set by a user that has seen the contents of the database,
but should not contain any sensitive data as Datafaker has not written any of the data (or summaries thereof)
into the file.

``config.yaml`` also contains human-readable descriptions of the summaries that will be produced,
but these will be copied into the summary file produced in the following stage, next to the actual summary data.

Datafaker summary stats phase
-----------------------------

``datafaker make-stats`` makes a file called ``src-stats.yaml``. This contains the summary data from the source database.

Information Governance should be focused on ensuring that private data is not leaking out in this file.

Obfuscation happens in this phase if it is configured; therefore this command represents
most of the Reduce and all of the Obfuscate phase.

Once these files, ``orm.yaml``, ``config.yaml`` and ``make-stats.yaml`` have been examined and approved for release,
we can move to the next phase.

Datafaker make-vocab phase
--------------------------

``datafaker make-vocab`` makes a whole set of ``.yaml`` or ``.yaml.gz`` files,
each of which represents the entire contents of one table in the source database.

The idea is that some tables will never contain sensitive data,
they might describe, for example, all the care centres that
are referenced in the database, or all the set of all possible
deseases that might be diagnosed (not who has such a diagnosis
or even if anybody has). Such tables are referred to as
Vocabulary tables.

Which tables are represented is configured by the ``configure-tables`` command,
so the user must be careful not to configure any tables containing sensitive
data as Vocabulary tables.

Releasing the intermediate data
-------------------------------

These files -- ``orm.yaml``, ``config.yaml``, the vocabulary files and especially
the ``src-stats.yaml`` file -- now need to have Information Governance
processes applied to them as it is these files that can be extracted from the
private network or Trusted Research Environment to allow the construction of
the synthetic data in a less sensitive computing environment, if required.

.. list-table:: Information Governance Classification of Each Datafaker Output
    :width: 100%
    :widths: 10 20 20 20 10 10 20

    * - Artefact
      - Derived from real data?
      - Contains patient-level data?
      - Granularity
      - Privacy risk
      - IG approval required?
      - Can leave TRE?
    * - ``orm.yaml``
      - Yes
      - No
      - Structural only
      - Low
      - Yes
      - Yes
    * - ``config.yaml``
      - User-authored
      - No
      - None
      - Low
      - No
      - Yes
    * - ``src-stats.yaml``
      - Yes
      - Occasion­ally
      - Aggregate
      - Medium
      - Yes
      - Condi­tion­al
    * - Vocabulary tables
      - Yes
      - No
      - Full table
      - None if correctly identified
      - Yes
      - Condi­tion­al
    * - Synthetic output (described below)
      - No
      - No
      - Patient-level synthetic data
      - Low
      - No
      - Yes

It is worh further elaborating on two of these boxes:
Firstly, ``src-stats.yaml`` "occasionally" contains patient-level data;
this is true if the table being summarized contains patient-level data
*and* the summarizing function is reporting on every value in one or more columns
*and* rare values are not being suppressed (leading to a value that applies
to just one or two individuals being released).
Search the ``src-stats.yaml`` file for comments such as:

    All the values that appear in column *column-name* of table *table-name*

or

    All the values that appear in column *column-name* of table *table-name*  more than 7 times

Secondly, Vocabulary Tables' privacy risk is "None if correctly identified".
A Vocabulary Table is supposed to be a table simply providing categories for other tables to reference.
They are not changed during the operation of the database and so releasing them does not represent a privacy risk.
However, there is some flexibility here; a list of care provider institutions is not technically a vocabulary table
but it is probably safe to treat it as one.
The important point is that Datafaker allows the user to specify any table as a vocabulary table;
if the user incorrectly specifies sensitive data as Vocabulary, it must not be released!

Datafaker Repopulate phase
--------------------------

Once we have released the summary data as described above we can operate outside of the TRE
as the sensitive data is no longer accessed by Datafaker.

The remaining commands are:

- ``datafaker create-tables`` creates the structure of the destination database to match (as much as is requested) the structure of the source database
- ``datafaker create-data`` writes fake data into the destination database.

As these operations require no access to the sensitive data, this phase can be
distributed to anybody that wants to make their own fake data in their own
database should we want to allow that. This could allow them to make
larger or smaller similar databases, or tweak the generated data with
the results of their own simulations in order to test their own analyses.
