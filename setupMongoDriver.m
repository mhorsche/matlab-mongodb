function setupMongoDriver()
%SETUPMONGODRIVER adds Mongo Java Driver downloaded by support software
%installer on java class path (JAVACLASSPATH.TXT present in MATLAB's
%preferences directory).
%
% Copyright 2017 The Mathworks, Inc.
% Updated to v3.12.0 (2019/12/24 by horsche)
%

installPath = fullfile(matlab.internal.get3pInstallLocation('mongodb.instrset'),'MongoDriver','mongo-java-driver-3.12.0.jar');

% Check if Mongo Java Driver is downloaded correctly by the Support
% Software Installer
if exist(installPath,'file') ~= 2
    error(message('mongodb:mongodb:locateDriverFailed'));
end

pathToJavaClassPath = fullfile(prefdir,'javaclasspath.txt');

% If path to 3p JAR file already exists in JAVACLASSPATH.TXT, no action is
% taken and return.
if installPathExists(pathToJavaClassPath,installPath)
    return;
end

% Create or append installation path to JAVACLASSPATH.TXT present in user's
% MATLAB preferences.
try
    fid = fopen(pathToJavaClassPath,'a+');
    fprintf(fid,"\n%s\n",installPath);
    fclose(fid);
catch
    error(message('mongodb:mongodb:configurationError'));
end

msgbox(message('mongodb:mongodb:restartMATLAB').getString,...
       message('mongodb:mongodb:restartRequired').getString, 'modal');

end

% helper function to check is installation path exists on JAVACLASSPATH.TXT
function exists = installPathExists(pathToJavaClassPath,installPath)

    fileexists = exist(pathToJavaClassPath,'file');
    exists = false;

    if fileexists ~= 2
        exists = false;
        return;
    end

    fid = fopen(pathToJavaClassPath,'r');

    while true
        str = string(fgetl(fid));
        if str.strlength == 0
            continue;
        end

        if str == "-1"
            break;
        end

        if str == installPath
            exists = true;
            break;
        end
    end

    fclose(fid);

end
