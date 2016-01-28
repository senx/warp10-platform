# WARP 10 IDE Setup & Build  

## IDE Setup with Eclipse

### Eclipse Configuration
In order to avoid conflicts between gradle & Eclipse, please configure Eclipse default output path on another directory BEFORE importing the gradle project(bin for example in Preferences/Java/Build Path) 

### Execute Gradle tasks
Generate Thrift source code with the tasks

    gradle warp10:generateThrift
    gradle token:generateThrift 

### Import the project
Import the project in Eclipse as Gradle project.
If build/gen-java are not compiled on warp10 & token subproject, refresh it manually. 

Warp10 should compile and run at this step.

### token or crypto not available on JCenter ???
It's possible on the head with DEV versions.

#### 1. install crypto on your local repo

    gradle crypto:install 


#### 2. Updates build.gradle
Updates crypto & token dependencies with the version of the artifact generated in the build.gradle (line 20) 

    warp10Version['crypto'] = '0.0.8-1024-g10a9ccc'
    warp10Version['token'] = '0.0.8-1024-g10a9ccc'
    
and activate MavenLocal() repository (uncomment it in the global repositories definition)     
    
#### 3. install token on  your local repo

    gradle token:install
    
#### 4. updates Eclipse External Dependencies
Warp10 Context Menu / Gradle / Refresh Gradle Project    
    
    
## IDE Setup with IntelliJ
TO complete

## RELEASE Procedure

The release & upload can only be performed on a clone with a git "porcelain" status (no new file or modifications) 

### Configuration

Add your API key & credentials in the gradle.properties file.

### Updates build dependencies

Update the components revision if needed with the new tag

    warp10Version['warp10'] = '0.0.2-rc1'
    warp10Version['warpscript'] = '0.0.2-rc1'
    warp10Version['hbaseFilters'] = '0.0.2-rc1'
    warp10Version['crypto'] = '0.0.2-rc1'
    warp10Version['token'] = '0.0.2-rc1'
    
 commit & push the change.

### GIT TAG

adds GIT tag `git tag -a 0.0.2-rc1 -m 'release candidate 0.0.2'`
WARNING don't forget to push the TAG `git push origin 0.0.2-rc1`

### Build Warp10
Build Warp10 in the order bellow

    gradle crypto:clean crypto:bintrayUpload   (if updated)
    gradle token:clean  token:bintrayUpload    (if updated) 
    gradle warp10:clean warp10:bintrayUpload



