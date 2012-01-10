Summary: ugent pbsscripts
Name: pbsscripts-ugent
Version: 0.5
Release: 3
License: GPL
Group: Applications/System
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
%description
Extra scripts for pbs interaction. (show_jobs, pbsmon, submitfilter, ...)

%prep
%setup -q

%build

%install
mkdir -p $RPM_BUILD_ROOT/usr/bin/
mkdir -p $RPM_BUILD_ROOT/var/spool/pbs/
install show_stats $RPM_BUILD_ROOT/usr/bin/
install show_jobs $RPM_BUILD_ROOT/usr/bin/
install show_nodes $RPM_BUILD_ROOT/usr/bin/
install show_queues $RPM_BUILD_ROOT/usr/bin/
install pbsmon $RPM_BUILD_ROOT/usr/bin/
install show_mem $RPM_BUILD_ROOT/usr/bin/
install submitfilter.py $RPM_BUILD_ROOT/var/spool/pbs/


%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/bin/show_stats
/usr/bin/show_jobs
/usr/bin/show_nodes
/usr/bin/show_queues
/usr/bin/pbsmon
/usr/bin/show_mem
/var/spool/pbs/submitfilter

%changelog
* Tue Jan 10 2012 Jens Timmerman <jens.timmerman@gmail.com>
- replaced submitfilter with python implementation, this fixes:
- submitfilter not giving priority to command line options (for ppn)
- submitfilter not detecting en of of header
* Thu May 12 2011 Wouter Depypere <wouter.depypere@ugent.be>
- first version

