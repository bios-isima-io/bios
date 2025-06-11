#!/bin/bash
jmeter_directory="./apache-jmeter-5.0"
jmeter_report_file="result.jlt"
jmeter_report_dir="./report"

if [ ! -f "./${jmeter_report_file}" ]
	then
		echo "${jmeter_report_file} is not present in current directory, aborting operation"
		exit 1
fi

# if report directory exists delete it because report can only be created in empty dir
if [ -d "${jmeter_report_dir}" ]
then
	rm -rf "${jmeter_report_dir}"
fi

echo "******************** Generating load test report ***************"
"${jmeter_directory}"/bin/jmeter -g ./"${jmeter_report_file}" -o "${jmeter_report_dir}"
echo "see index.html in ${jmeter_report_dir} for load test report"