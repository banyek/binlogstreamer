# How to build mambo

For building this project you can simply use GNU make.

### Build the mambo for the current architecture
If you want to build mambo for your current architecture, you can do that with a simple make command. 
This will download all the dependencies, and builds mambo binary

    > make

### Download dependencies
If you want to download all the dependencies for development, but you don't want to build the app, just make deps

    > make deps

### Build for OSX (darwin)
You can explicit tell if the build target is OSX. 

    > make darwin

### Build for Linux
You can explicit tell if the build target is Linux. 

    > make linux
    
### Remove all files  
If you have changes in the code, before commiting to github you should remove all the files which were created during build.

    > make clean
