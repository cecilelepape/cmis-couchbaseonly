This project is a simple demo to show how to build a Content Management system flexible and scalable using Couchbase as the repository.  I did a previous implem where Couchbase is only used to store metadata. The storage can be either local in your filesystem or remote using AWS. The purpose of this project is to show that one can also use Couchbase to store the binary files themselves, with our own chunk manager.

For more info check on http://cecilelepape.blogspot.fr/2016/01/storing-blobs-in-couchbase-for-content.html

Get the code from chemistry-opencmis-server-couchbaseonly and edit the properties you can find in src/main/webapp/WEB-INF/classes/repository.properties

- Edit the login with the credentials that must match your Web server's one. I use Apache Tomcat so I edited the conf/tomcat-users.xml to add test/test user.
- Edit the location of your couchbase server (localhost by default)
- Create a bucket to store your metadata in Couchbase. By default the bucket must be named cmismeta.
- Create a bucket to store your binary files in Couchbase. By default the bucket must be named cmistore.

Compile the code and deploy the war in your Web server as couchbase.war

To create folders or documents into your Apache Chemistry server, you can use the workbench provided by Apache Chemistry. You can found the instructions in here : http://chemistry.apache.org/java/developing/tools/dev-tools-workbench.html

To connect to the repository, set the URL to http://localhost:8080/couchbase/atom11, set the credentials (test/test by default) and the repository name (test).
