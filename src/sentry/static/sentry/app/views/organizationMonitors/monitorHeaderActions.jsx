import PropTypes from 'prop-types';
import React from 'react';
import {browserHistory} from 'react-router';

import {t} from 'app/locale';
import {
  addErrorMessage,
  addLoadingMessage,
  removeIndicator,
} from 'app/actionCreators/indicator';
import {logException} from 'app/utils/logging';
import SentryTypes from 'app/sentryTypes';
import Button from 'app/components/button';
import Confirm from 'app/components/confirm';
import withApi from 'app/utils/withApi';

class MonitorHeaderActions extends React.Component {
  static propTypes = {
    monitor: SentryTypes.Monitor.isRequired,
    orgId: PropTypes.string.isRequired,
    onUpdate: PropTypes.func,
  };

  handleDelete = () => {
    const {orgId, monitor} = this.props;
    const redirectPath = `/organizations/${orgId}/monitors/`;
    addLoadingMessage(t('Deleting Monitor...'));

    this.api
      .requestPromise(`/monitors/${monitor.id}/`, {
        method: 'DELETE',
      })
      .then(() => {
        browserHistory.push(redirectPath);
      })
      .catch(() => {
        removeIndicator();
        addErrorMessage(t('Unable to remove monitor.'));
      });
  };

  updateMonitor = data => {
    const {monitor} = this.props;
    addLoadingMessage();
    this.api
      .requestPromise(`/monitors/${monitor.id}/`, {
        method: 'PUT',
        data,
      })
      .then(resp => {
        removeIndicator();
        this.props.onUpdate && this.props.onUpdate(resp);
      })
      .catch(err => {
        logException(err);
        removeIndicator();
        addErrorMessage(t('Unable to update monitor.'));
      });
  };

  toggleStatus = () => {
    const {monitor} = this.props;
    this.updateMonitor({
      status: monitor.status === 'disabled' ? 'active' : 'disabled',
    });
  };

  render() {
    const {monitor, orgId} = this.props;
    return (
      <div className="m-b-1">
        <div className="btn-group">
          <Button
            size="small"
            icon="icon-edit"
            to={`/organizations/${orgId}/monitors/${monitor.id}/edit/`}
          >
            {t('Edit')}
          </Button>
        </div>
        <div className="btn-group" style={{marginLeft: 10}}>
          <Button size="small" icon="icon-edit" onClick={this.toggleStatus}>
            {monitor.status !== 'disabled' ? t('Pause') : t('Enable')}
          </Button>
        </div>
        <div className="btn-group" style={{marginLeft: 10}}>
          <Confirm
            onConfirm={this.handleDelete}
            message={t(
              'Deleting this monitor is permanent. Are you sure you wish to continue?'
            )}
          >
            <Button size="small" icon="icon-trash">
              {t('Delete')}
            </Button>
          </Confirm>
        </div>
      </div>
    );
  }
}

export default withApi(MonitorHeaderActions);
