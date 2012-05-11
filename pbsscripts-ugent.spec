Summary: ugent pbsscripts
Name: pbsscripts-ugent
Version: 1.0
Release: 1
License: GPL
Group: Applications/System
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Requires: python-vsc-packages-utils >= 0.6.0

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
install submitfilter $RPM_BUILD_ROOT/var/spool/pbs/


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
* Thu May 24 2012 Jens Timmerman <jens.timmerman@gmail.com>
- Submitfilter: changed they way vmem etc is computed, taking new swap into account.
* Wed Feb 08 2012 Jens Timmerman <jens.timmerman@gmail.com>
- added support for b,tb,w,kw,mw,gw and tw as suffix for vmem specification.
* Tue Jan 10 2012 Jens Timmerman <jens.timmerman@gmail.com>
- replaced submitfilter with python implementation, this fixes:
- submitfilter not giving priority to command line options (for ppn)
- submitfilter not detecting end of header
* Thu May 12 2011 Wouter Depypere <wouter.depypere@ugent.be>
- first version

