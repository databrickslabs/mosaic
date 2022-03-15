==================
Installation guide
==================


Pre-requisites
**************

In order to use Mosaic, you must have access to a Databricks cluster running
Databricks Runtime 9.1 LTS. If you have cluster creation permissions in your Databricks workspace, you
can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`__.

You will also need "Can Manage" permissions on this cluster in order to attach the
Mosaic library to your cluster. A workspace administrator will be able to grant 
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`__.

Installation
************

.. note::
   The advice here will change when Mosaic is officially released.
   Check back regularly for updates to the installation process.

Mosaic is packaged as a Python `Wheel <https://www.python.org/dev/peps/pep-0427/>`_.

To install Mosaic on your Databricks cluster, take the following steps:

#. Download the Python .whl file from the Mosaic repository.
#. Attach the file to the cluster following the instructions `here <https://docs.databricks.com/libraries/cluster-libraries.html#cluster-installed-library>`__.

Testing
*******

To test the installation, create a new Python notebook and import the package as follows:

.. code-block:: python

    import mosaic
    mosaic.__version__
