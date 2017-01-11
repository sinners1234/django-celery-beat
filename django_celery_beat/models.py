"""Database models."""
from __future__ import absolute_import, unicode_literals

from datetime import timedelta

from django.core.exceptions import MultipleObjectsReturned, ValidationError
from django.db import models
from django.db.models import signals
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from celery import schedules
from celery.five import python_2_unicode_compatible

from celery.schedules import BaseSchedule, schedstate
from celery.utils.time import (
    weekday, maybe_timedelta, remaining, humanize_seconds,
    timezone, maybe_make_aware, ffwd, localize
)


import pytz
import datetime

from . import managers
from .utils import now

DAYS = 'days'
HOURS = 'hours'
MINUTES = 'minutes'
SECONDS = 'seconds'
MICROSECONDS = 'microseconds'

PERIOD_CHOICES = (
    (DAYS, _('Days')),
    (HOURS, _('Hours')),
    (MINUTES, _('Minutes')),
    (SECONDS, _('Seconds')),
    (MICROSECONDS, _('Microseconds')),
)

SOLAR_SCHEDULES = [(x, _(x)) for x in schedules.solar._all_events]


def cronexp(field):
    """Representation of cron expression."""
    return field and str(field).replace(' ', '') or '*'


@python_2_unicode_compatible
class SolarSchedule(models.Model):
    """Schedule following astronomical patterns."""

    event = models.CharField(
        _('event'), max_length=24, choices=SOLAR_SCHEDULES
    )
    latitude = models.DecimalField(
        _('latitude'), max_digits=9, decimal_places=6
    )
    longitude = models.DecimalField(
        _('longitude'), max_digits=9, decimal_places=6
    )

    class Meta:
        """Table information."""

        verbose_name = _('solar event')
        verbose_name_plural = _('solar events')
        ordering = ('event', 'latitude', 'longitude')
        unique_together = ('event', 'latitude', 'longitude')

    @property
    def schedule(self):
        return schedules.solar(self.event, self.latitude, self.longitude)

    @classmethod
    def from_schedule(cls, schedule):
        spec = {'event': schedule.event,
                'latitude': schedule.lat,
                'longitude': schedule.lon}
        try:
            return cls.objects.get(**spec)
        except cls.DoesNotExist:
            return cls(**spec)
        except MultipleObjectsReturned:
            cls.objects.filter(**spec).delete()
            return cls(**spec)

    def __str__(self):
        return '{0} ({1}, {2})'.format(
            self.get_event_display(),
            self.latitude,
            self.longitude
        )


@python_2_unicode_compatible
class IntervalSchedule(models.Model):
    """Schedule executing every n seconds."""

    DAYS = DAYS
    HOURS = HOURS
    MINUTES = MINUTES
    SECONDS = SECONDS
    MICROSECONDS = MICROSECONDS

    PERIOD_CHOICES = PERIOD_CHOICES

    every = models.IntegerField(_('every'), null=False)
    period = models.CharField(
        _('period'), max_length=24, choices=PERIOD_CHOICES,
    )

    class Meta:
        """Table information."""

        verbose_name = _('interval')
        verbose_name_plural = _('intervals')
        ordering = ['period', 'every']

    @property
    def schedule(self):
        return schedules.schedule(timedelta(**{self.period: self.every}))

    @classmethod
    def from_schedule(cls, schedule, period=SECONDS):
        every = max(schedule.run_every.total_seconds(), 0)
        try:
            return cls.objects.get(every=every, period=period)
        except cls.DoesNotExist:
            return cls(every=every, period=period)
        except MultipleObjectsReturned:
            cls.objects.filter(every=every, period=period).delete()
            return cls(every=every, period=period)

    def __str__(self):
        if self.every == 1:
            return _('every {0.period_singular}').format(self)
        return _('every {0.every} {0.period}').format(self)

    @property
    def period_singular(self):
        return self.period[:-1]
        
@python_2_unicode_compatible
class IntervalStartDateSchedule(IntervalSchedule):
    """Schedule executing every n seconds."""

    DAYS = DAYS
    HOURS = HOURS
    MINUTES = MINUTES
    SECONDS = SECONDS
    MICROSECONDS = MICROSECONDS

    PERIOD_CHOICES = PERIOD_CHOICES

    start_date = models.DateTimeField()
    
    class Meta:
        """Table information."""

        verbose_name = _('interval')
        verbose_name_plural = _('intervals')
        ordering = ['period', 'every']

    @property
    def schedule(self):
        print ("CALLED")
        return StartDateSchedule(run_every=timedelta(**{self.period: self.every}), start_date=self.start_date)

    @classmethod
    def from_schedule(cls, schedule, period=SECONDS):
        print ("CALLED TWO")
        every = max(schedule.run_every.total_seconds(), 0)
        start_date = schedule.start_date
        try:
            return cls.objects.get(every=every, period=period, start_date=start_date)
        except cls.DoesNotExist:
            return cls(every=every, period=period, start_date=start_date)
        except MultipleObjectsReturned:
            cls.objects.filter(every=every, period=period, start_date=start_date).delete()
            return cls(every=every, period=period, start_date=start_date)

    def __str__(self):
        if self.every == 1:
            return _('every {0.period_singular}').format(self)
        return _('every {0.every} {0.period}').format(self)

    @property
    def period_singular(self):
        return self.period[:-1]


@python_2_unicode_compatible
class CrontabSchedule(models.Model):
    """Crontab-like schedule."""

    minute = models.CharField(_('minute'), max_length=64, default='*')
    hour = models.CharField(_('hour'), max_length=64, default='*')
    day_of_week = models.CharField(
        _('day of week'), max_length=64, default='*',
    )
    day_of_month = models.CharField(
        _('day of month'), max_length=64, default='*',
    )
    month_of_year = models.CharField(
        _('month of year'), max_length=64, default='*',
    )

    class Meta:
        """Table information."""

        verbose_name = _('crontab')
        verbose_name_plural = _('crontabs')
        ordering = ['month_of_year', 'day_of_month',
                    'day_of_week', 'hour', 'minute']

    def __str__(self):
        return '{0} {1} {2} {3} {4} (m/h/d/dM/MY)'.format(
            cronexp(self.minute),
            cronexp(self.hour),
            cronexp(self.day_of_week),
            cronexp(self.day_of_month),
            cronexp(self.month_of_year),
        )

    @property
    def schedule(self):
        return schedules.crontab(minute=self.minute,
                                 hour=self.hour,
                                 day_of_week=self.day_of_week,
                                 day_of_month=self.day_of_month,
                                 month_of_year=self.month_of_year)

    @classmethod
    def from_schedule(cls, schedule):
        spec = {'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year}
        try:
            return cls.objects.get(**spec)
        except cls.DoesNotExist:
            return cls(**spec)
        except MultipleObjectsReturned:
            cls.objects.filter(**spec).delete()
            return cls(**spec)
            

class PeriodicTasks(models.Model):
    """Helper table for tracking updates to periodic tasks."""

    ident = models.SmallIntegerField(default=1, primary_key=True, unique=True)
    last_update = models.DateTimeField(null=False)

    objects = managers.ExtendedManager()

    @classmethod
    def changed(cls, instance, **kwargs):
        if not instance.no_changes:
            cls.update_changed()

    @classmethod
    def update_changed(cls, **kwargs):
        cls.objects.update_or_create(ident=1, defaults={'last_update': now()})

    @classmethod
    def last_change(cls):
        try:
            return cls.objects.get(ident=1).last_update
        except cls.DoesNotExist:
            pass


@python_2_unicode_compatible
class PeriodicTask(models.Model):
    """Model representing a periodic task."""

    name = models.CharField(
        _('name'), max_length=200, unique=True,
        help_text=_('Useful description'),
    )
    task = models.CharField(_('task name'), max_length=200)
    interval = models.ForeignKey(
        IntervalSchedule,
        null=True, blank=True, verbose_name=_('interval'),
    )
    crontab = models.ForeignKey(
        CrontabSchedule, null=True, blank=True, verbose_name=_('crontab'),
        help_text=_('Use one of interval/crontab'),
    )
    solar = models.ForeignKey(
        SolarSchedule, null=True, blank=True, verbose_name=_('solar'),
        help_text=_('Use a solar schedule')
    )
    args = models.TextField(
        _('Arguments'), blank=True, default='[]',
        help_text=_('JSON encoded positional arguments'),
    )
    kwargs = models.TextField(
        _('Keyword arguments'), blank=True, default='{}',
        help_text=_('JSON encoded keyword arguments'),
    )
    queue = models.CharField(
        _('queue'), max_length=200, blank=True, null=True, default=None,
        help_text=_('Queue defined in CELERY_TASK_QUEUES'),
    )
    exchange = models.CharField(
        _('exchange'), max_length=200, blank=True, null=True, default=None,
    )
    routing_key = models.CharField(
        _('routing key'), max_length=200, blank=True, null=True, default=None,
    )
    expires = models.DateTimeField(
        _('expires'), blank=True, null=True,
    )
    enabled = models.BooleanField(
        _('enabled'), default=True,
    )
    last_run_at = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
    )
    total_run_count = models.PositiveIntegerField(
        default=0, editable=False,
    )
    date_changed = models.DateTimeField(auto_now=True)
    description = models.TextField(_('description'), blank=True)

    objects = managers.PeriodicTaskManager()
    no_changes = False

    class Meta:
        """Table information."""

        verbose_name = _('periodic task')
        verbose_name_plural = _('periodic tasks')

    def validate_unique(self, *args, **kwargs):
        super(PeriodicTask, self).validate_unique(*args, **kwargs)
        if not self.interval and not self.crontab and not self.solar:
            raise ValidationError({
                'interval': [
                    'One of interval, crontab, or solar must be set.'
                ]
            })
        if self.interval and self.crontab and self.solar:
            raise ValidationError({
                'crontab': [
                    'Only one of interval, crontab, or solar must be set'
                ]
            })

    def save(self, *args, **kwargs):
        self.exchange = self.exchange or None
        self.routing_key = self.routing_key or None
        self.queue = self.queue or None
        if not self.enabled:
            self.last_run_at = None
        super(PeriodicTask, self).save(*args, **kwargs)

    def __str__(self):
        fmt = '{0.name}: {{no schedule}}'
        if self.interval:
            fmt = '{0.name}: {0.interval}'
        if self.crontab:
            fmt = '{0.name}: {0.crontab}'
        if self.solar:
            fmt = '{0.name}: {0.solar}'
        return fmt.format(self)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        if self.crontab:
            return self.crontab.schedule
            
# probably doesnt belong here but where else should it go?           
class StartDateSchedule(BaseSchedule):
    def __init__(self, run_every=None, start_date=None, relative=False, nowfun=None, app=None):
        self.run_every = maybe_timedelta(run_every)
        self.start_date=start_date
        self.relative=relative
        self.has_run=False
        super(StartDateSchedule, self).__init__(nowfun=nowfun, app=app)
    
    def remaining_estimate(self, last_run_at):
        return remaining(
            self.maybe_make_aware(last_run_at), self.run_every,
            self.maybe_make_aware(self.now()), self.relative,
        )
        
    def is_due(self, last_run_at):
        now = datetime.datetime.now(tz=pytz.utc)
        
        if now >= self.start_date:
            if self.has_run==False:
                self.has_run=True
                return schedstate(is_due=True, next=self.seconds)
                
            
            last_run_at = self.maybe_make_aware(last_run_at)
            rem_delta = self.remaining_estimate(last_run_at)
            remaining_s = max(rem_delta.total_seconds(), 0)
            if remaining_s == 0:
                return schedstate(is_due=True, next=self.seconds)
            return schedstate(is_due=False, next=remaining_s)
        else:
            return schedstate(is_due=False, next=(self.start_date-now).total_seconds())
            

    def __repr__(self):
        return '<freq: {0.human_seconds}>'.format(self)

    def __eq__(self, other):
        if isinstance(other, StartDateSchedule):
            return self.run_every == other.run_every
        return self.run_every == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __reduce__(self):
        return self.__class__, (self.run_every, self.start_date, self.relative, self.nowfun)

    @property
    def seconds(self):
        return max(self.run_every.total_seconds(), 0)

    @property
    def human_seconds(self):
        return humanize_seconds(self.seconds)


signals.pre_delete.connect(PeriodicTasks.changed, sender=PeriodicTask)
signals.pre_save.connect(PeriodicTasks.changed, sender=PeriodicTask)
signals.pre_delete.connect(
    PeriodicTasks.update_changed, sender=IntervalSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=IntervalSchedule)
signals.post_delete.connect(
    PeriodicTasks.update_changed, sender=CrontabSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=CrontabSchedule)
signals.post_delete.connect(
    PeriodicTasks.update_changed, sender=SolarSchedule)
signals.post_save.connect(
    PeriodicTasks.update_changed, sender=SolarSchedule)
